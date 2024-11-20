import asyncio
import posixpath
import shlex
import stat
import weakref
from contextlib import AsyncExitStack, suppress
from datetime import datetime
from typing import Optional

import asyncssh
from asyncssh.sftp import SFTPOpUnsupported
from fsspec.asyn import AsyncFileSystem, async_methods, sync, sync_wrapper
from fsspec.utils import infer_storage_options

from sshfs.file import SSHFile
from sshfs.pools import SFTPSoftChannelPool
from sshfs.utils import (
    READ_BLOCK_SIZE,
    WRITE_BLOCK_SIZE,
    as_progress_handler,
    wrap_exceptions,
)

async_methods.append("_mv")

# Always allocate 2 channels for shell operations
# and the rest (generally 8) for SFTP.
_SHELL_CHANNELS = 2
_DEFAULT_MAX_SESSIONS = 10


class SSHFileSystem(AsyncFileSystem):
    def __init__(
        self,
        host,
        *,
        pool_type=SFTPSoftChannelPool,
        sftp_client_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        """
        Implementation of the SFTP/SSH protocols for the fsspec.

        Parameters
        ----------
        host: str
            SSH host to connect.
        **kwargs: Any
            Any option that will be passed to either the top level
            `AsyncFileSystem` (e.g. timeout)
            or the `asyncssh.connect`.
        pool_type: sshfs.pools.base.BaseSFTPChannelPool
            Pool manager to use (when doing concurrent operations together,
            pool managers offer the flexibility of prioritizing channels
            and deciding which to use).
        sftp_client_kwargs: Optional[dict]
            Parameters to pass to asyncssh.SSHClientConnection.start_sftp_client method
            (e.g. env, send_env, path_encoding, path_errors, sftp_version).
        """

        super().__init__(self, **kwargs)

        _timeout = kwargs.pop("timeout", None)
        max_sessions = kwargs.pop("max_sessions", _DEFAULT_MAX_SESSIONS)
        if max_sessions <= _SHELL_CHANNELS:
            raise ValueError(
                f"max_sessions must be greater than {_SHELL_CHANNELS}"
            )
        _client_args = kwargs.copy()
        _client_args.setdefault("known_hosts", None)
        sftp_client_kwargs = sftp_client_kwargs or {}

        self._stack = AsyncExitStack()
        self.active_executors = 0
        self._client, self._pool = self.connect(
            host,
            pool_type,
            max_sftp_channels=max_sessions - _SHELL_CHANNELS,
            timeout=_timeout,  # goes to sync_wrapper
            connect_args=_client_args,  # for asyncssh.connect
            sftp_client_kwargs=sftp_client_kwargs,  # for asyncssh.SSHClientConnection.start_sftp_client
        )
        weakref.finalize(
            self, sync, self.loop, self._finalize, self._pool, self._stack
        )

    @classmethod
    def _strip_protocol(cls, path):
        # Remove components such as host and username from path.
        inferred_path = infer_storage_options(path)["path"]
        return super()._strip_protocol(inferred_path)

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        out = infer_storage_options(urlpath)
        out.pop("path", None)
        out.pop("protocol", None)
        return out

    @wrap_exceptions
    async def _connect(
        self,
        host,
        pool_type,
        max_sftp_channels,
        connect_args,
        sftp_client_kwargs,
    ):
        self._client_lock = asyncio.Semaphore(_SHELL_CHANNELS)

        _raw_client = asyncssh.connect(host, **connect_args)
        client = await self._stack.enter_async_context(_raw_client)
        pool = pool_type(
            client,
            max_channels=max_sftp_channels,
            sftp_client_kwargs=sftp_client_kwargs,
        )
        return client, pool

    connect = sync_wrapper(_connect)

    async def _finalize(self, pool, stack):
        await pool.close()

        # If an error occurs while the SSHFile is trying to
        # open the native file, then the client might get broken
        # due to partial initalization. We are just going to ignore
        # the errors that arises on the finalization layer
        with suppress(BrokenPipeError):
            await stack.aclose()

    @property
    def client(self):
        assert self._client is not None
        return self._client

    def _decode_attributes(self, attributes):
        if stat.S_ISDIR(attributes.permissions):
            kind = "directory"
        elif stat.S_ISREG(attributes.permissions):
            kind = "file"
        elif stat.S_ISLNK(attributes.permissions):
            kind = "link"
        else:
            kind = "unknown"

        return {
            "size": attributes.size,
            "type": kind,
            "gid": attributes.gid,
            "uid": attributes.uid,
            "time": datetime.utcfromtimestamp(attributes.atime),
            "mtime": datetime.utcfromtimestamp(attributes.mtime),
            "permissions": attributes.permissions,
        }

    @wrap_exceptions
    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        async with self._pool.get() as channel:
            attributes = await channel.stat(path)

        info = self._decode_attributes(attributes)
        path = path.rstrip("/")
        if info["type"] == "directory":
            path += "/"
        info["name"] = path
        return info

    @wrap_exceptions
    async def _modified(self, path: str, **kwargs) -> datetime:
        path_info = await self._info(path)
        return path_info["mtime"]

    modified = sync_wrapper(_modified)

    @wrap_exceptions
    async def _mv(self, lpath, rpath, **kwargs):
        async with self._pool.get() as channel:
            with suppress(SFTPOpUnsupported):
                return await channel.posix_rename(lpath, rpath)

        # Some systems doesn't natively support posix_rename
        # which is an extension to the original SFTP protocol.
        # In that case we are going to copy the file and delete
        # it.

        try:
            await self._cp_file(lpath, rpath)
        finally:
            await self._rm_file(lpath)

    @wrap_exceptions
    async def _put_file(
        self,
        lpath,
        rpath,
        block_size=WRITE_BLOCK_SIZE,
        callback=None,
        **kwargs,
    ):
        await self._makedirs(self._parent(rpath), exist_ok=True)
        async with self._pool.get() as channel:
            await channel.put(
                lpath,
                rpath,
                block_size=block_size,
                progress_handler=as_progress_handler(callback),
            )

    @wrap_exceptions
    async def _get_file(
        self, lpath, rpath, block_size=READ_BLOCK_SIZE, callback=None, **kwargs
    ):
        async with self._pool.get() as channel:
            await channel.get(
                lpath,
                rpath,
                block_size=block_size,
                progress_handler=as_progress_handler(callback),
            )

    @wrap_exceptions
    async def _cp_file(self, lpath, rpath, **kwargs):
        cmd = f"cp {shlex.quote(lpath)} {shlex.quote(rpath)}"
        await self._execute(cmd)

    @wrap_exceptions
    async def _ls(self, path, detail=False, **kwargs):
        async with self._pool.get() as channel:
            file_attrs = await channel.readdir(path)

        infos = []
        for file_attr in file_attrs:
            if file_attr.filename in ["", ".", ".."]:
                continue
            info = self._decode_attributes(file_attr.attrs)
            info["name"] = posixpath.join(path, file_attr.filename)
            infos.append(info)

        # TODO: listings cache
        if detail:
            return infos
        else:
            return [info["name"] for info in infos]

    @wrap_exceptions
    async def _mkdir(
        self, path, *, create_parents=True, permissions=511, **kwargs
    ):
        if create_parents:
            return await self._makedirs(path, exist_ok=True)

        attrs = asyncssh.SFTPAttrs(permissions=permissions)
        async with self._pool.get() as channel:
            await channel.mkdir(path, attrs=attrs)

    @wrap_exceptions
    async def _makedirs(
        self, path, *, exist_ok=False, permissions=511, **kwargs
    ):
        attrs = asyncssh.SFTPAttrs(permissions=permissions)
        async with self._pool.get() as channel:
            await channel.makedirs(path, exist_ok=exist_ok, attrs=attrs)

    mkdir = sync_wrapper(_mkdir)
    makedirs = sync_wrapper(_makedirs)

    @wrap_exceptions
    async def _rm_file(self, path, **kwargs):
        async with self._pool.get() as channel:
            await channel.unlink(path)

    @wrap_exceptions
    async def _rmdir(
        self,
        path,
        recursive=False,
        ignore_errors=False,
        on_error=None,
        **kwargs,
    ):
        async with self._pool.get() as channel:
            if recursive:
                await channel.rmtree(
                    path, ignore_errors=ignore_errors, onerror=on_error
                )
            else:
                await channel.rmdir(path)

    async def _rm(self, path, recursive=False, **kwargs):
        if isinstance(path, str):
            path = [path]

        coros = []
        for sub_path in path:
            if await self._isdir(sub_path):
                coro = self._rmdir(sub_path, recursive, **kwargs)
            else:
                coro = self._rm_file(sub_path)
            coros.append(coro)

        await asyncio.gather(*coros)

    @wrap_exceptions
    async def _checksum(self, path):
        system = await self._get_system()
        if system == "Linux":
            command = "md5sum"
            part = 0
        elif system == "Darwin":
            command = "md5"
            part = -1
        else:
            raise ValueError(f"{system!r} doesn't support checksum operation")

        cmd = f"{command} {shlex.quote(path)}"
        result = await self._execute(cmd)

        parts = result.stdout.strip().split()
        assert len(parts) >= 1

        checksum = parts[part]
        assert len(checksum) == 32
        return checksum

    @wrap_exceptions
    async def _get_system(self):
        result = await self._execute("uname")
        return result.stdout.strip()

    checksum = sync_wrapper(_checksum)
    get_system = sync_wrapper(_get_system)

    async def _execute(self, *args, **kwargs):
        """Execute a shell command on the host system
        and return the result."""
        kwargs.setdefault("check", True)
        async with self._client_lock:
            return await self.client.run(*args, **kwargs)

    execute = sync_wrapper(_execute)

    def _open(self, path, *args, **kwargs):
        return SSHFile(self, path, *args, **kwargs)

    @wrap_exceptions
    async def _cat_file(self, path, **kwargs):
        """Asynchronously fetch the contents of a file"""
        async with self._pool.get() as channel:
            async with channel.open(path, "rb") as f:
                return await f.read()

    @wrap_exceptions
    async def _pipe_file(self, path, data, chunksize=50 * 2**20, **kwargs):
        """Asynchronously writes the given data to a remote file in chunks."""
        await self._makedirs(self._parent(path), exist_ok=True)

        async with self._pool.get() as channel:
            async with channel.open(path, "wb") as f:
                for i in range(0, len(data), chunksize):
                    chunk = data[i : i + chunksize]
                    await f.write(chunk)

        self.invalidate_cache(path)
