import asyncio
import functools
import time

import aioredis_cluster
import aredis
import uvloop

import redis.asyncio as redispy


def timer(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        tic = time.perf_counter()
        await func(*args, **kwargs)
        toc = time.perf_counter()
        return f"{toc - tic:.4f}"

    return wrapper


@timer
async def warmup(client):
    await asyncio.gather(
        *(asyncio.create_task(client.exists(f"bench:warmup_{i}")) for i in range(100))
    )


@timer
async def run(client):
    data_str = "a" * size
    data_int = int("1" * size)

    for i in range(count):
        with client.pipeline() as pipe:
            await (
                pipe.set(f"bench:str_{i}", data_str)
                .set(f"bench:int_{i}", data_int)
                .get(f"bench:str_{i}")
                .get(f"bench:int_{i}")
                .hset("bench:hset", str(i), data_str)
                .hget("bench:hset", str(i))
                .incr("bench:incr")
                .lpush("bench:lpush", data_int)
                .lrange("bench:lpush", 0, 300)
                .lpop("bench:lpush")
                .execute()
            )


async def main(loop):
    arc = aredis.StrictRedisCluster(
        host=host,
        port=port,
        password=password,
        max_connections=2**31,
        max_connections_per_node=2**31,
        readonly=False,
        reinitialize_steps=count,
        skip_full_coverage_check=True,
        decode_responses=False,
        max_idle_time=count,
        idle_check_interval=count,
    )
    print(f"{loop} {await warmup(arc)} aredis")
    print(await run(arc))
    arc.connection_pool.disconnect()

    aiorc = await aioredis_cluster.create_redis_cluster(
        [(host, port)],
        password=password,
        state_reload_interval=count,
        idle_connection_timeout=count,
        pool_maxsize=2**31,
    )
    print(f"{loop} {await warmup(aiorc)} aioredis-cluster")
    print(await run(aiorc))
    aiorc.close()
    await aiorc.wait_closed()

    async with redispy.RedisCluster(
        host=host,
        port=port,
        password=password,
        reinitialize_steps=count,
        read_from_replicas=False,
        decode_responses=False,
        max_connections=2**31,
    ) as rca:
        print(f"{loop} {await warmup(rca)} redispy")
        print(await run(rca))


if __name__ == "__main__":
    host = "localhost"
    port = 16379
    password = None

    count = 10000
    size = 256

    asyncio.run(main("asyncio"))

    uvloop.install()

    asyncio.run(main("uvloop"))
