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
async def set_str(client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.set(f"bench:str_{i}", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.set(f"bench:str_{i}", data)


@timer
async def set_int(client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.set(f"bench:int_{i}", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.set(f"bench:int_{i}", data)


@timer
async def get_str(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.get(f"bench:str_{i}")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.get(f"bench:str_{i}")


@timer
async def get_int(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.get(f"bench:int_{i}")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.get(f"bench:int_{i}")


@timer
async def hset(client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.hset("bench:hset", str(i), data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.hset("bench:hset", str(i), data)


@timer
async def hget(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.hget("bench:hset", str(i)))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.hget("bench:hset", str(i))


@timer
async def incr(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.incr("bench:incr")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.incr("bench:incr")


@timer
async def lpush(client, gather, data):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.lpush("bench:lpush", data))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.lpush("bench:lpush", data)


@timer
async def lrange_300(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(
                    asyncio.create_task(client.lrange("bench:lpush", i, i + 300))
                    for i in range(100)
                )
            )
    else:
        for i in range(count):
            await client.lrange("bench:lpush", i, i + 300)


@timer
async def lpop(client, gather):
    if gather:
        for _ in range(count // 100):
            await asyncio.gather(
                *(asyncio.create_task(client.lpop("bench:lpush")) for i in range(100))
            )
    else:
        for i in range(count):
            await client.lpop("bench:lpush")


@timer
async def warmup(client):
    await asyncio.gather(
        *(asyncio.create_task(client.exists(f"bench:warmup_{i}")) for i in range(100))
    )


@timer
async def run(client, gather):
    data_str = "a" * size
    data_int = int("1" * size)

    if gather is False:
        for ret in await asyncio.gather(
            asyncio.create_task(set_str(client, gather, data_str)),
            asyncio.create_task(set_int(client, gather, data_int)),
            asyncio.create_task(hset(client, gather, data_str)),
            asyncio.create_task(incr(client, gather)),
            asyncio.create_task(lpush(client, gather, data_int)),
        ):
            print(ret)
        for ret in await asyncio.gather(
            asyncio.create_task(get_str(client, gather)),
            asyncio.create_task(get_int(client, gather)),
            asyncio.create_task(hget(client, gather)),
            asyncio.create_task(lrange_300(client, gather)),
            asyncio.create_task(lpop(client, gather)),
        ):
            print(ret)
    else:
        print(await set_str(client, gather, data_str))
        print(await set_int(client, gather, data_int))
        print(await hset(client, gather, data_str))
        print(await incr(client, gather))
        print(await lpush(client, gather, data_int))

        print(await get_str(client, gather))
        print(await get_int(client, gather))
        print(await hget(client, gather))
        print(await lrange_300(client, gather))
        print(await lpop(client, gather))


async def main(loop, gather=None):
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
    print(f"{loop} {gather} {await warmup(arc)} aredis")
    print(await run(arc, gather=gather))
    arc.connection_pool.disconnect()

    aiorc = await aioredis_cluster.create_redis_cluster(
        [(host, port)],
        password=password,
        state_reload_interval=count,
        idle_connection_timeout=count,
        pool_maxsize=2**31,
    )
    print(f"{loop} {gather} {await warmup(aiorc)} aioredis-cluster")
    print(await run(aiorc, gather=gather))
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
        print(f"{loop} {gather} {await warmup(rca)} redispy")
        print(await run(rca, gather=gather))


if __name__ == "__main__":
    host = "localhost"
    port = 16379
    password = None

    count = 10000
    size = 256

    asyncio.run(main("asyncio"))
    asyncio.run(main("asyncio", gather=False))
    asyncio.run(main("asyncio", gather=True))

    uvloop.install()

    asyncio.run(main("uvloop"))
    asyncio.run(main("uvloop", gather=False))
    asyncio.run(main("uvloop", gather=True))
