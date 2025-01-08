import getpass
from contextlib import suppress
from pathlib import Path
import asyncssh

from asyncssh.config import SSHClientConfig

SSH_CONFIG = Path("~", ".ssh", "config").expanduser()


def parse_config(*, host, user=(), port=(), local_user=None, config_files=None):
    if config_files is None:
        config_files = [SSH_CONFIG]

    if local_user is None:
        with suppress(OSError):  # Use OSError as getuser() might raise this.
            local_user = getpass.getuser()

    last_config = None
    reload = False

    # Check asyncssh version
    version = tuple(map(int, asyncssh.__version__.split(".")))
    if version <= (2, 18, 0):  # Compare version properly
        return SSHClientConfig.load(
            last_config,
            config_files,
            reload,
            local_user,
            user,
            host,
            port,
        )
    canonical = False  # Fixed typo
    final = False  # Fixed typo
    return  SSHClientConfig.load(
        last_config,
        config_files,
        reload,
        canonical,  # Use correct parameter
        final,  # Use correct parameter
        local_user,
        user,
        host,
        port,
    )
