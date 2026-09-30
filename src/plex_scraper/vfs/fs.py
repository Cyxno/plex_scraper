"""FUSE layer (FASE 8): the stable Plex path.

The tree is rendered from the item registry — paths can only come from
registered logical items, so path traversal is impossible by construction.
open() pins a source generation via POST /media/{id}/open; reads stream
through the resolver; release() unpins. A dead source surfaces as EIO, the
next open resolves the next generation behind the SAME path.
"""
from __future__ import annotations

import errno
import logging
import os
import stat
import time

import httpx
import pyfuse3
import trio

log = logging.getLogger("vfs")

_REFRESH_TTL = 5.0


class _VfsNode:
    __slots__ = ("is_dir", "item_id", "size", "name")

    def __init__(self, is_dir: bool, name: str, item_id: str | None = None, size: int = 0):
        self.is_dir = is_dir
        self.name = name
        self.item_id = item_id
        self.size = size


class PlexScraperFs(pyfuse3.Operations):
    def __init__(self, resolver_url: str):
        super().__init__()
        self._client = httpx.AsyncClient(base_url=resolver_url, timeout=30.0)
        self._tree: dict[str, _VfsNode] = {}        # path -> node
        self._children: dict[str, list[str]] = {}   # path -> child paths (sorted)
        self._path_inode: dict[str, int] = {"": pyfuse3.ROOT_INODE}
        self._inode_path: dict[int, str] = {pyfuse3.ROOT_INODE: ""}
        self._next_ino = 2
        self._tree_built_at = 0.0
        self._build_lock = trio.Lock()
        self._open_handles: dict[int, str] = {}     # fh -> resolver handle
        self._next_fh = [1]
        self.root_attrs()

    # ------------------------------------------------------------- tree
    def root_attrs(self) -> pyfuse3.EntryAttributes:
        ent = pyfuse3.EntryAttributes()
        ent.st_ino = pyfuse3.ROOT_INODE
        ent.generation = 0
        ent.entry_timeout = 2.0
        ent.attr_timeout = 2.0
        ent.st_mode = stat.S_IFDIR | 0o755
        ent.st_nlink = 2
        ent.st_uid, ent.st_gid = 0, 0
        ent.st_rdev = 0
        ent.st_size = 0
        ent.st_blksize = 512
        ent.st_blocks = 0
        ent.st_atime_ns = ent.st_mtime_ns = ent.st_ctime_ns = int(time.time() * 1e9)
        return ent

    async def _ensure_tree(self, force: bool = False) -> None:
        async with self._build_lock:
            if not force and time.monotonic() - self._tree_built_at < _REFRESH_TTL:
                return
            try:
                resp = await self._client.get("/media")
                resp.raise_for_status()
                items = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("resolver unreachable: %r", exc)
                if not self._tree:
                    raise pyfuse3.FUSEError(errno.EIO)
                return                                  # serve stale tree
            tree: dict[str, _VfsNode] = {"": _VfsNode(True, "")}
            children: dict[str, list[str]] = {}
            path_inode: dict[str, int] = {"": pyfuse3.ROOT_INODE}
            inode_path: dict[int, str] = {pyfuse3.ROOT_INODE: ""}
            next_ino = 2

            def register(path: str, node: _VfsNode) -> None:
                nonlocal next_ino
                tree[path] = node
                if path not in path_inode:
                    ino = next_ino
                    next_ino += 1
                    path_inode[path] = ino
                    inode_path[ino] = path
                parent = path.rsplit("/", 1)[0] if "/" in path else ""
                children.setdefault(parent, [])
                if path not in children[parent]:
                    children[parent].append(path)

            for item in items:
                parts = [p for p in item["plex_path"].split("/") if p]
                if not parts:
                    continue
                for i in range(1, len(parts)):
                    register("/".join(parts[:i]), _VfsNode(True, parts[i - 1]))
                register("/".join(parts),
                         _VfsNode(False, parts[-1], item["id"], int(item.get("size") or 0)))
            for parent, kids in children.items():
                kids.sort()
            self._tree, self._children = tree, children
            self._path_inode, self._inode_path = path_inode, inode_path
            self._next_ino = next_ino
            self._tree_built_at = time.monotonic()
            log.info("tree_refreshed items=%d paths=%d", len(items), len(tree) - 1)

    # --------------------------------------------------------- inode utils
    def _attrs(self, node: _VfsNode, ino: int) -> pyfuse3.EntryAttributes:
        ent = self.root_attrs()
        ent.st_ino = ino
        if node.is_dir:
            ent.st_mode = stat.S_IFDIR | 0o755
            ent.st_nlink = 2
        else:
            ent.st_mode = stat.S_IFREG | 0o644
            ent.st_nlink = 1
            ent.st_size = node.size
            ent.st_blocks = (node.size + 511) // 512
        return ent

    # ----------------------------------------------------- pyfuse3 interface
    async def lookup(self, parent_inode: int, name, ctx=None) -> pyfuse3.EntryAttributes:
        if isinstance(name, bytes):
            name = name.decode()
        if parent_inode != pyfuse3.ROOT_INODE:
            parent_path = self._inode_path.get(parent_inode)
        else:
            parent_path = ""
        if parent_path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if name == ".":
            path = parent_path
        elif name == "..":
            path = parent_path.rsplit("/", 1)[0] if "/" in parent_path else ""
        else:
            path = f"{parent_path}/{name}" if parent_path else name
        await self._ensure_tree(force=path not in self._tree)
        node = self._tree.get(path)
        if node is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return self._attrs(node, self._path_inode[path])

    async def getattr(self, inode: int, ctx=None) -> pyfuse3.EntryAttributes:
        path = self._inode_path.get(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        await self._ensure_tree(force=path not in self._tree)
        node = self._tree.get(path)
        if node is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return self._attrs(node, self._path_inode[path])

    async def setattr(self, inode, attr, fields, fh, ctx=None) -> pyfuse3.EntryAttributes:
        # read-only filesystem: only reflect current attrs
        return await self.getattr(inode)

    async def opendir(self, inode: int, ctx=None) -> int:
        path = self._inode_path.get(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        await self._ensure_tree(force=path not in self._tree)
        if path not in self._children and path != "":
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        return inode

    async def readdir(self, fh: int, off: int, token):
        path = self._inode_path.get(fh, "")
        kids = self._children.get(path, [])
        entries = [(".", fh), ("..", self._parent_ino(path))] + \
                  [(p.rsplit("/", 1)[-1], self._path_inode[p]) for p in kids]
        for i in range(max(off, 0), len(entries)):
            name, ino = entries[i]
            node = self._tree.get(path if name in (".", "..") else
                                  (f"{path}/{name}" if path else name))
            if node is None:
                continue
            if not pyfuse3.readdir_reply(token, name.encode(), self._attrs(node, ino), i + 1):
                break

    def _parent_ino(self, path: str) -> int:
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        return self._path_inode.get(parent, pyfuse3.ROOT_INODE)

    async def open(self, inode: int, flags: int, ctx=None) -> int:
        path = self._inode_path.get(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        node = self._tree.get(path)
        if node is None or node.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR if node and node.is_dir else errno.ENOENT)
        if node.item_id is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if flags & os.O_WRONLY or flags & os.O_RDWR or flags & os.O_TRUNC:
            raise pyfuse3.FUSEError(errno.EACCES)
        try:
            resp = await self._client.post(f"/media/{node.item_id}/open")
        except httpx.HTTPError:
            raise pyfuse3.FUSEError(errno.EIO)
        if resp.status_code == 503:
            raise pyfuse3.FUSEError(errno.ENODATA)     # item exists, no source yet
        if resp.status_code != 200:
            raise pyfuse3.FUSEError(errno.EIO)
        handle = resp.json()["handle"]
        fh = self._next_fh[0]
        self._next_fh[0] += 1
        self._open_handles[fh] = handle
        info = pyfuse3.FileInfo(fh)
        # direct_io: a new handle must never see a previous generation's bytes
        # out of the kernel page cache after a source switch.
        info.direct_io = True
        info.keep_cache = False
        return info

    async def read(self, fh: int, off: int, size: int) -> bytes:
        handle = self._open_handles.get(fh)
        if handle is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        try:
            resp = await self._client.get(
                f"/stream/{handle}", params={"offset": off, "length": size})
        except httpx.HTTPError as exc:
            log.warning("read transport error handle=%s off=%d: %r", handle, off, exc)
            raise pyfuse3.FUSEError(errno.EIO)
        if resp.status_code != 200:
            log.warning("read failed handle=%s off=%d size=%d http=%d",
                        handle, off, size, resp.status_code)
            raise pyfuse3.FUSEError(errno.EIO)
        return resp.content

    async def release(self, fh: int) -> None:
        handle = self._open_handles.pop(fh, None)
        if handle is not None:
            try:
                await self._client.delete(f"/open/{handle}")
            except httpx.HTTPError:
                pass

    async def flush(self, fh: int) -> None:
        pass

    async def fsync(self, fh: int, datasync: bool) -> None:
        pass

    async def access(self, inode: int, mode: int, ctx=None) -> bool:
        return True

    # ------------------------------------------------------------- teardown
    async def shutdown(self) -> None:
        for fh in list(self._open_handles):
            await self.release(fh)
        await self._client.aclose()


def mount_main(mountpoint: str, resolver_url: str, allow_other: bool = True) -> None:
    """Blocking entrypoint: runs the FUSE session on the trio loop (pyfuse3
    3.5 main() is trio-based; httpx is anyio-based and runs natively)."""
    import signal


    async def _run() -> None:
        fs = PlexScraperFs(resolver_url)
        # NB: entry/attr timeouts are per-EntryAttributes; pyfuse3 3.5 has no
        # max_read negotiation, so the kernel default read size applies.
        options = ["fsname=plex_scraper"]
        if allow_other:
            options.append("allow_other")
        pyfuse3.init(fs, mountpoint, options)
        logging.getLogger("vfs").info("mounted %s", mountpoint)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(pyfuse3.main)
                try:
                    with trio.open_signal_receiver(
                            signal.SIGINT, signal.SIGTERM) as signals:
                        async for _sig in signals:
                            pyfuse3.terminate()
                            break
                except RuntimeError:
                    # non-main thread (tests): no signal handling; the session
                    # ends when the mount is unmounted externally
                    await trio.sleep_forever()
        finally:
            try:
                pyfuse3.close(unmount=True)
            except Exception as exc:               # mount may already be gone
                logging.getLogger("vfs").warning("unmount failed: %r", exc)
            await fs.shutdown()

    trio.run(_run)
