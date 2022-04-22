from typing import List, Optional, Union

from redis.asyncio.client import Redis
from redis.exceptions import RedisError, ResponseError


class CommandsParser:
    """
    Parses Redis commands to get command keys.

    COMMAND output is used to determine key locations.
    Commands that do not have a predefined key location are flagged with 'movablekeys',
    and these commands' keys are determined by the command 'COMMAND GETKEYS'.

    NOTE: Due to a bug in redis<7.0, this does not work properly
    for EVAL or EVALSHA when the `numkeys` arg is 0.
     - issue: https://github.com/redis/redis/issues/9493
     - fix: https://github.com/redis/redis/pull/9733

    So, don't use this with EVAL or EVALSHA.
    """

    __slots__ = ("commands",)

    def __init__(self) -> None:
        self.commands = {}

    async def initialize(self, r: Redis) -> None:
        commands = await r.execute_command("COMMAND")
        uppercase_commands = []
        for cmd in commands:
            if any(x.isupper() for x in cmd):
                uppercase_commands.append(cmd)
        for cmd in uppercase_commands:
            commands[cmd.lower()] = commands.pop(cmd)

        for cmd, command in commands.items():
            if "movablekeys" in command["flags"]:
                commands[cmd] = -1
            elif command["first_key_pos"] == 0 and command["last_key_pos"] == 0:
                commands[cmd] = 0
            elif command["first_key_pos"] == 1 and command["last_key_pos"] == 1:
                commands[cmd] = 1
        self.commands = commands

    # As soon as this PR is merged into Redis, we should reimplement
    # our logic to use COMMAND INFO changes to determine the key positions
    # https://github.com/redis/redis/pull/8324
    async def get_keys(
        self, redis_conn: Redis, *args
    ) -> Optional[Union[List[str], List[bytes]]]:
        if len(args) < 2:
            # The command has no keys in it
            return None

        try:
            command = self.commands[args[0].lower()]
        except KeyError:
            # try to split the command name and to take only the main command
            # e.g. 'memory' for 'memory usage'
            cmd_name_split = args[0].lower().split()
            cmd_name = cmd_name_split[0]
            if cmd_name in self.commands:
                # save the splitted command to args
                args = cmd_name_split + list(args[1:])
            else:
                # We'll try to reinitialize the commands cache, if the engine
                # version has changed, the commands may not be current
                await self.initialize(redis_conn)
                if cmd_name not in self.commands:
                    raise RedisError(
                        f"{cmd_name.upper()} command doesn't exist in Redis commands"
                    )

            command = self.commands[cmd_name]

        if command == 1:
            return [args[1]]
        if command == 0:
            return None
        if command == -1:
            return await self._get_moveable_keys(redis_conn, *args)

        last_key_pos = command["last_key_pos"]
        if last_key_pos < 0:
            last_key_pos = len(args) + last_key_pos
        return args[command["first_key_pos"] : last_key_pos + 1 : command["step_count"]]

    async def _get_moveable_keys(self, redis_conn: Redis, *args) -> Optional[List[str]]:
        pieces = []
        cmd_name = args[0]
        # The command name should be splitted into separate arguments,
        # e.g. 'MEMORY USAGE' will be splitted into ['MEMORY', 'USAGE']
        pieces = pieces + cmd_name.split()
        pieces = pieces + list(args[1:])
        try:
            keys = await redis_conn.execute_command("COMMAND GETKEYS", *pieces)
        except ResponseError as e:
            message = e.__str__()
            if (
                "Invalid arguments" in message
                or "The command has no key arguments" in message
            ):
                return None
            else:
                raise e
        return keys
