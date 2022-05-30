import sys

from base import Benchmark

from redis.commands.packer import SPEEDUPS, CommandPacker
from redis.encoder import Encoder


class CommandsPackerBenchmark(Benchmark):
    ARGUMENTS = (
        {"name": "speedups", "values": [True]},
        {"name": "value_size", "values": [10, 100, 1000, 10000, 100000]},
    )

    def setup(self, speedups, value_size):
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=True
        )
        self.packer = CommandPacker(encoder=encoder, speedups=speedups)
        self.commands = []

        self.commands.append(("SET", "benchmark", "value" * value_size))
        self.commands.append(("GET", "benchmark"))
        self.commands.append(("HSET", "benchmark", "field", b"value" * value_size))
        self.commands.append(("HGETALL", "benchmark"))
        self.commands.append(
            (
                "MSET",
                "benchmark1",
                min(int("1" * value_size), sys.maxsize),
                "benchmark2",
                min(float("1" * value_size) + 0.1 + 0.2, sys.float_info.max),
            )
        )
        self.commands.append(("MGET", "benchmark1", "benchmark2"))
        self.commands.append(("SET", "benchmark", memoryview(b"value" * value_size)))
        self.commands.append(("DEL", "benchmark", "benchmark1", "benchmark2"))
        self.commands.append(("DEL", "benchmark", {"benchmark1", "benchmark2"}))

    def run(self, speedups, value_size):
        self.packer.pack_commands(args for args in self.commands)


if __name__ == "__main__":
    if SPEEDUPS:
        CommandsPackerBenchmark().run_benchmark()
    else:
        print("Speedups not installed. Aborting.")
