import asyncio
from contextlib import AsyncExitStack, suppress
from typing import Optional

from asyncssh.misc import ChannelOpenError

MAX_TIMEOUT = 60 * 60 * 3


class BaseSFTPChannelPool:
    """BaseSFTPChannelPool is a pool manager for SFTP channels created
    by asyncssh client. The pool might operate in two different modes
    depending on the subclass."""

    def __init__(
        self,
        client,
        *,
        max_channels: Optional[int] = None,
        timeout: int = MAX_TIMEOUT,
        unsafe_terminate: bool = True,
        sftp_client_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        self.client = client

        # This limit might change during the execution to reflect
        # better to the server's capacity to prevent getting too
        # many errors and wasting time on creating failed channels.
        self.max_channels = max_channels

        # When there are no channels available, this is the maximum amount
        # of time that the SFTPChannelPool will wait to retrieve the
        # channel. If nothing gets released within this parameter, then
        # a TimeoutError will be raised. It can be None.
        self.timeout = timeout

        # When the pool is closing, whether to terminate all open
        # connections or raise an error to indicate there are leaks.
        self.unsafe_terminate = unsafe_terminate
        self._stack = AsyncExitStack()

        self.sftp_client_kwargs = sftp_client_kwargs or {}

    async def _maybe_new_channel(self):
        # If there is no hard limit or the limit is not hit yet
        # try to create a new channel
        if (
            self.max_channels is None
            or self.active_channels < self.max_channels
        ):
            try:
                return await self._stack.enter_async_context(
                    self.client.start_sftp_client(**self.sftp_client_kwargs)
                )
            except ChannelOpenError:
                # If we can't create any more channels, then change
                # the hard limit to reflect that so that we don't hit
                # these errors again.
                self.max_channels = self.active_channels

    async def get(self):
        raise NotImplementedError

    async def _cleanup(self):
        ...

    async def close(self):
        if self.active_channels and not self.unsafe_terminate:
            raise RuntimeError(
                f"{type(self).__name__!r} can't be closed while there are active channels"
            )

        async with asyncio.Lock():
            with suppress(Exception):
                await self._cleanup()

            await self._stack.aclose()
