from base import Benchmark

from redis.connection import PythonParser, _HiredisParser


class SocketReadBenchmark(Benchmark):

    ARGUMENTS = (
        {"name": "parser", "values": [PythonParser, _HiredisParser]},
        {
            "name": "value_size",
            "values": [10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000],
        },
        {"name": "read_size", "values": [4096, 8192, 16384, 32768, 65536, 131072]},
    )

    def setup(self, value_size, read_size, parser):
        r = self.get_client(parser_class=parser, socket_read_size=read_size)
        r.set("benchmark", "a" * value_size)

    def run(self, value_size, read_size, parser):
        r = self.get_client()
        r.get("benchmark")


if __name__ == "__main__":
    SocketReadBenchmark().run_benchmark()
