"""
A tiny TCP proxy that sits between redis-py and a real Redis server.

For the first RESET_COUNT connections it accepts, it forces a TCP RST
(via SO_LINGER with linger=0) instead of a clean close or a normal
handshake -- this is what makes the client see a real
`ConnectionResetError`, at the OS/socket level, not a Python-level mock.

After RESET_COUNT resets, every subsequent connection is transparently
proxied through to the real backend Redis server.

Usage:
    python proxy.py --listen-port 6500 --backend-port 6399 --reset-count 2
"""
import argparse
import asyncio
import socket
import struct


async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter):
    try:
        while True:
            data = await src.read(4096)
            if not data:
                break
            dst.write(data)
            await dst.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        dst.close()


def make_handler(backend_host, backend_port, reset_count, state):
    async def handle(reader, writer):
        state["attempt"] += 1
        n = state["attempt"]

        if n <= reset_count:
            print(f"[proxy] attempt {n}: forcing RST (simulated reset)", flush=True)
            sock = writer.get_extra_info("socket")
            # linger=0 close sends a raw RST instead of a clean FIN
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            writer.close()
            return

        print(f"[proxy] attempt {n}: passing through to real backend", flush=True)
        try:
            backend_reader, backend_writer = await asyncio.open_connection(
                backend_host, backend_port
            )
        except OSError as e:
            print(f"[proxy] could not reach backend: {e}", flush=True)
            writer.close()
            return

        await asyncio.gather(
            pipe(reader, backend_writer),
            pipe(backend_reader, writer),
        )

    return handle


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--backend-port", type=int, required=True)
    parser.add_argument("--backend-host", default="127.0.0.1")
    parser.add_argument("--reset-count", type=int, default=2)
    args = parser.parse_args()

    state = {"attempt": 0}
    handler = make_handler(
        args.backend_host, args.backend_port, args.reset_count, state
    )
    server = await asyncio.start_server(handler, "127.0.0.1", args.listen_port)
    print(
        f"[proxy] listening on 127.0.0.1:{args.listen_port} -> "
        f"{args.backend_host}:{args.backend_port} "
        f"(first {args.reset_count} attempts will be RST)",
        flush=True,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
