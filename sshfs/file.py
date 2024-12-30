import io

from asyncssh.sftp import _MAX_SFTP_REQUESTS
from fsspec.asyn import sync

from sshfs.utils import (
    READ_BLOCK_SIZE,
    WRITE_BLOCK_SIZE,
    _mirror_method,
    wrap_exceptions,
)


class SSHFile(io.IOBase):
    def __init__(
        self, fs, path, mode="rb", block_size=None, max_requests=None, **kwargs
    ):
        self.fs = fs
        self.loop = fs.loop

        if "t" in mode or "b" not in mode:
            raise ValueError(f"Unsupported file mode: {mode}")

        self.path = path
        self.mode = mode
        self.max_requests = max_requests or _MAX_SFTP_REQUESTS

        # The blocksize is often used with constructs like
        # shutil.copyfileobj(src, dst, length=file.blocksize) and since we are
        # using pipelining, we are going to reflect the total size rather than
        # a size of chunk to our limits.
        self.blocksize = (
            None if block_size is None else block_size * self.max_requests
        )

        self.kwargs = kwargs

        self._file = sync(self.loop, self._open_file)
        self._closed = False

    def _determine_block_size(self, channel):
        # Use the asyncssh block sizes to ensure the best performance.
        limits = getattr(channel, "limits", None)
        if limits:
            if self.readable():
                return limits.max_read_len
            return limits.max_write_len

        # "The OpenSSH SFTP server will close the connection
        # if it receives a message larger than 256 KB, and
        # limits read requests to returning no more than
        # 64 KB."
        #
        # We are going to use the maximum block_size possible
        # with a 16KB margin (so instead of sending 256 KB data,
        # we'll send 240 KB + headers for write requests)
        return READ_BLOCK_SIZE if self.readable() else WRITE_BLOCK_SIZE

    @wrap_exceptions
    async def _open_file(self):
        # TODO: this needs to keep a reference to the
        # pool as well, otherwise we might broke our
        # guarantee for the hard pool since the file
        # will still be using that channel to perform
        # it's operations but the pool it thinking this
        # channel is freed.
        async with self.fs._pool.get() as channel:
            if self.blocksize is None:
                self.blocksize = (
                    self._determine_block_size(channel) * self.max_requests
                )
            return await channel.open(
                self.path,
                self.mode,
                block_size=self.blocksize // self.max_requests,
                max_requests=self.max_requests,
            )

    read = _mirror_method("read")
    seek = _mirror_method("seek")
    tell = _mirror_method("tell")

    write = _mirror_method("write")
    fsync = _mirror_method("fsync")
    truncate = _mirror_method("truncate")

    _close = _mirror_method("close")

    def readable(self):
        return "r" in self.mode or "+" in self.mode

    def seekable(self):
        return "r" in self.mode or "w" in self.mode

    def seekable(self):
        return True

    def seekable(self):
        return "r" in self.mode or "w" in self.mode

    def writable(self):
        return any(x in self.mode for x in ["a", "w", "+"])

    def close(self):
        if self._closed:
            return None

        self._close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    @property
    def closed(self):
        return self._closed
