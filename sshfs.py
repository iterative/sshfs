import asyncio
import errno
import functools
import heapq
import os
import posixpath
import secrets
import shlex
import stat
import weakref
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import datetime

import asyncssh
from asyncssh import ProcessError
from asyncssh.misc import ChannelOpenError, PermissionDenied
from asyncssh.sftp import SFTPFailure, SFTPNoSuchFile, SFTPOpUnsupported
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.spec import AbstractBufferedFile

_UNSET = object()
_NOT_FOUND = os.strerror(errno.ENOENT)
_FILE_EXISTS = os.strerror(errno.EEXIST)


def _drop_unset(namespace):
    return {
        key: value for key, value in namespace.items() if value is not _UNSET
    }


def _get_tmp_file(path):
    return posixpath.join(path, f".tmp.{secrets.token_hex(16)}")


def wrap_exceptions(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except PermissionDenied as exc:
            raise PermissionError(exc.reason) from exc
        except SFTPNoSuchFile as exc:
            raise FileNotFoundError(errno.ENOENT, _NOT_FOUND) from exc
        except ProcessError as exc:
            message = exc.stderr.strip()
            if message.endswith(_NOT_FOUND):
                raise FileNotFoundError(errno.ENOENT, _NOT_FOUND) from exc
            raise
        except SFTPFailure as exc:
            message = exc.reason
            if message.endswith("already exists"):
                raise FileExistsError(errno.EEXIST, _FILE_EXISTS) from exc
            raise

    return wrapper


_MAX_TIMEOUT = 60 * 60 * 3


class _SFTPChannelPool:
    """_SFTPChannelPool is a pool manager for SFTP channels created
    by asyncssh client. The pool might operate in two different modes
    depending on the subclass."""

    def __init__(
        self,
        client,
        *,
        max_channels=None,
        timeout=_MAX_TIMEOUT,
        unsafe_terminate=True,
    ):
        self.client = client

        # This limit might change during the execution to reflect
        # better to the server's capacity to prevent getting too
        # many errors and wasting time on creating failed channels.
        self.max_channels = max_channels
        self.active_channels = 0

        # When there are no channels available, this is the maximum amount
        # of time that the SFTPChannelPool will wait to retrieve the
        # channel. If nothing gets released within this parameter, then
        # a TimeoutError will be raised. It can be None.
        self.timeout = timeout

        # When the pool is closing, whether to terminate all open
        # connections or raise an error to indicate there are leaks.
        self.unsafe_terminate = unsafe_terminate
        self._stack = AsyncExitStack()

    async def _maybe_new_channel(self):
        # If there is no hard limit or the limit is not hit yet
        # try to create a new channel
        if (
            self.max_channels is None
            or self.active_channels < self.max_channels
        ):
            try:
                return await self._stack.enter_async_context(
                    self.client.start_sftp_client()
                )
            except ChannelOpenError:
                # If we can't create any more channels, then change
                # the hard limit to reflect that so that we don't hit
                # these errors again.
                self.max_channels = self.active_channels

    def get(self):
        raise NotImplementedError

    async def _cleanup(self):
        raise NotImplementedError

    async def close(self):
        if self.active_channels and not self.unsafe_terminate:
            raise RuntimeError(
                f"{type(self).__name__!r} can't be closed while there are active channels"
            )

        async with asyncio.Lock():
            with suppress(Exception):
                await self._cleanup()

            await self._stack.aclose()


class SFTPHardChannelPool(_SFTPChannelPool):
    """A _SFTPChannelPool implementation that ensures at any moment in time,
    a single SFTP channel might only be used by a single coroutine. When there
    are no more active channels, the ``.get()`` method will block for a channel
    to get released (if ``timeout`` is specified, it will poll for ``timeout``
    seconds until a ``TimeoutError`` is raised)."""

    def __init__(self, *args, **kwargs):
        self._queue = asyncio.Queue(0)
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def get(self):
        channel = None
        if self._queue.empty():
            channel = await self._maybe_new_channel()

        if channel is None:
            channel = await asyncio.wait_for(
                self._queue.get(), timeout=self.timeout
            )

        self.active_channels += 1
        yield channel
        self.active_channels -= 1
        self._queue.put_nowait(channel)

    async def _cleanup(self):
        while not self._queue.empty():
            self._queue.get_nowait()


class SFTPSoftChannelPool(_SFTPChannelPool):
    """A _SFTPChannelPool implementation that allows usage of same channels
    by multiple coroutines and handles the balanced distribution of multiple
    channels from least used to most used. The ``.get()`` method will not block
    unlike the hard pool and no timeouts will happen on the management side."""

    # Placeholder to use when there are no channels in
    # the counter.
    _NO_CHANNELS = [[None, 1]]

    def __init__(self, *args, **kwargs):
        self._channels = Counter()
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def get(self):
        [(least_used_channel, num_connections)] = (
            heapq.nsmallest(1, self._channels.items(), lambda kv: kv[1])
            or self._NO_CHANNELS
        )

        if num_connections > 0:
            channel = await self._maybe_new_channel()
            if channel is not None:
                least_used_channel = channel
                num_connections = 0

        if least_used_channel is None:
            raise ValueError("Can't create any SFTP connections!")

        self._channels[least_used_channel] += 1
        self.active_channels += 1
        yield least_used_channel
        self._channels[least_used_channel] -= 1
        self.active_channels -= 1

    async def _cleanup(self):
        self._channels.clear()


class SSHFileSystem(AsyncFileSystem):
    def __init__(
        self,
        host,
        *,
        port=_UNSET,
        username=_UNSET,
        password=_UNSET,
        client_keys=_UNSET,
        known_hosts=_UNSET,
        sftp_channel_pool="soft",
        max_sftp_channels=_UNSET,
        max_sftp_channel_wait_timeout=_UNSET,
        **kwargs,
    ):
        super().__init__(self, **kwargs)

        self._client_args = _drop_unset(
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "client_keys": client_keys,
                "known_hosts": known_hosts,
            }
        )
        if sftp_channel_pool == "soft":
            self._pool_type = SFTPSoftChannelPool
        elif sftp_channel_pool == "hard":
            self._pool_type = SFTPHardChannelPool
        else:
            raise ValueError(f"Unknown pool type: {sftp_channel_pool!r}")

        self._pool_args = _drop_unset(
            {
                "max_channels": max_sftp_channels,
                "timeout": max_sftp_channel_wait_timeout,
            }
        )

        self._stack = AsyncExitStack()
        self._client, self._pool = self.connect()
        weakref.finalize(
            self, sync, self.loop, self._finalize, self._pool, self._stack
        )

    @wrap_exceptions
    async def _connect(self):
        _raw_client = asyncssh.connect(**self._client_args)
        client = await self._stack.enter_async_context(_raw_client)
        pool = self._pool_type(client, **self._pool_args)
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
        async with self._pool.get() as channel:
            attributes = await channel.stat(path)

        info = self._decode_attributes(attributes)
        info["name"] = path
        return info

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
    async def _cp_file(self, lpath, rpath, **kwargs):
        cmd = f"cp {shlex.quote(lpath)} {shlex.quote(rpath)}"
        await self.client.run(cmd, check=True)

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
            await channel.mkdir(path)

    @wrap_exceptions
    async def _makedirs(
        self, path, *, exist_ok=False, permissions=511, **kwargs
    ):
        attrs = asyncssh.SFTPAttrs(permissions=permissions)
        async with self._pool.get() as channel:
            await channel.makedirs(path, exist_ok=exist_ok, attrs=attrs)

    makedirs = sync_wrapper(_makedirs)

    @wrap_exceptions
    async def _rm_file(self, path, **kwargs):
        async with self._pool.get() as channel:
            await channel.unlink(path)

    @wrap_exceptions
    async def _rmdir(self, path, **kwargs):
        async with self._pool.get() as channel:
            await channel.rmdir(path)

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
        result = await self.client.run(cmd, check=True)

        parts = result.stdout.strip().split()
        assert len(parts) >= 1

        checksum = parts[part]
        assert len(checksum) == 32
        return checksum

    @wrap_exceptions
    async def _get_system(self):
        result = await self.client.run("uname", check=True)
        return result.stdout.strip()

    checksum = sync_wrapper(_checksum)
    get_system = sync_wrapper(_get_system)

    def _open(self, path, mode="rb", **kwargs):
        return SSHFile(self, path, mode, **kwargs)


class SSHFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        self._file = None
        super().__init__(fs, path, mode, **kwargs)

        self.loop = self.fs.loop
        if self.mode not in {"rb", "wb"}:
            raise ValueError(f"Unsupported file open mode: {self.mode!r}")

        if "w" in self.mode:
            self._location = _get_tmp_file(self.fs._parent(self.path))
        else:
            self._location = self.path

        self._stack = AsyncExitStack()
        weakref.finalize(self, sync, self.loop, self._stack.aclose)

    @wrap_exceptions
    async def _async_open_file(self):
        channel = await self._stack.enter_async_context(self.fs._pool.get())
        # TODO: maybe pass block_size?
        return await self._stack.enter_async_context(
            channel.open(self._location, self.mode)
        )

    async def _async_close_file(self):
        await self._stack.aclose()
        self._file = None

    _open_file = sync_wrapper(_async_open_file)
    _close_file = sync_wrapper(_async_close_file)

    async def _async_fetch_range(self, start, end):
        if self._file is None:
            self._file = await self._async_open_file()

        await self._file.seek(start)
        return await self._file.read(end - start)

    _fetch_range = sync_wrapper(_async_fetch_range)

    async def _async_upload_chunk(self, final=False):
        if self._file is None:
            self._file = await self._async_open_file()

        await self._file.write(self.buffer.getvalue())

        if self.autocommit and final:
            await self._commit()
            await self._async_close_file()
            return True

    _upload_chunk = sync_wrapper(_async_upload_chunk)

    async def _commit(self):
        if "w" not in self.mode:
            return None

        await self.fs._mv(self._location, self.path)

    async def _flush(self, force=False):
        super().flush(force=force)
        self._file.fsync()

    commit = sync_wrapper(_commit)

    def close(self):
        # When the object is getting finalized, might
        # raise some errors due to missing properties
        # (eg stack or loop), so just ignore them since
        # our finalization is handled by the weakref.finalize
        with suppress(Exception):
            self._close_file()
        super().close()