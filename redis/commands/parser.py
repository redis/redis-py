from redis.exceptions import RedisError, ResponseError
from redis.utils import str_if_bytes


class CommandsParser:
    """
    Parses Redis commands to get command keys.
    COMMAND output is used to determine key locations.
    Commands that do not have a predefined key location are flagged with
    'movablekeys', and these commands' keys are determined by the command
    'COMMAND GETKEYS'.
    """

    def __init__(self, redis_connection):
        self.initialized = False
        self.commands = {}
        self.initialize(redis_connection)

    def initialize(self, r):
        self.commands = r.execute_command("COMMAND")

    # As soon as this PR is merged into Redis, we should reimplement
    # our logic to use COMMAND INFO changes to determine the key positions
    # https://github.com/redis/redis/pull/8324
    def get_keys(self, redis_conn, *args):
        """
        Get the keys from the passed command
        """
        if len(args) < 2:
            # The command has no keys in it
            return None

        cmd_name = args[0].lower()
        if cmd_name not in self.commands:
            # try to split the command name and to take only the main command,
            # e.g. 'memory' for 'memory usage'
            cmd_name_split = cmd_name.split()
            cmd_name = cmd_name_split[0]
            if cmd_name in self.commands:
                # save the splitted command to args
                args = cmd_name_split + list(args[1:])
            else:
                # We'll try to reinitialize the commands cache, if the engine
                # version has changed, the commands may not be current
                self.initialize(redis_conn)
                if cmd_name not in self.commands:
                    raise RedisError(
                        f"{cmd_name.upper()} command doesn't exist in Redis commands"
                    )

        command = self.commands.get(cmd_name)
        if "movablekeys" in command["flags"]:
            keys = self._get_moveable_keys(redis_conn, *args)
        elif "pubsub" in command["flags"]:
            keys = self._get_pubsub_keys(*args)
        else:
            if (
                command["step_count"] == 0
                and command["first_key_pos"] == 0
                and command["last_key_pos"] == 0
            ):
                # The command doesn't have keys in it
                return None
            last_key_pos = command["last_key_pos"]
            if last_key_pos < 0:
                last_key_pos = len(args) - abs(last_key_pos)
            keys_pos = list(
                range(command["first_key_pos"], last_key_pos + 1, command["step_count"])
            )
            keys = [args[pos] for pos in keys_pos]

        return keys

    def _get_moveable_keys(self, redis_conn, *args):
        pieces = []
        cmd_name = args[0]
        # The command name should be splitted into separate arguments,
        # e.g. 'MEMORY USAGE' will be splitted into ['MEMORY', 'USAGE']
        pieces = pieces + cmd_name.split()
        pieces = pieces + list(args[1:])
        try:
            keys = redis_conn.execute_command("COMMAND GETKEYS", *pieces)
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

    def _get_pubsub_keys(self, *args):
        """
        Get the keys from pubsub command.
        Although PubSub commands have predetermined key locations, they are not
        supported in the 'COMMAND's output, so the key positions are hardcoded
        in this method
        """
        if len(args) < 2:
            # The command has no keys in it
            return None
        args = [str_if_bytes(arg) for arg in args]
        command = args[0].upper()
        keys = None
        if command == "PUBSUB":
            # the second argument is a part of the command name, e.g.
            # ['PUBSUB', 'NUMSUB', 'foo'].
            pubsub_type = args[1].upper()
            if pubsub_type in ["CHANNELS", "NUMSUB"]:
                keys = args[2:]
        elif command in ["SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE"]:
            # format example:
            # SUBSCRIBE channel [channel ...]
            keys = list(args[1:])
        elif command == "PUBLISH":
            # format example:
            # PUBLISH channel message
            keys = [args[1]]
        return keys
