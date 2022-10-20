import redis.asyncio as redis
import asyncio
from redis.commands.json.path import Path as Jsonpath

host = "127.0.0.1"
port = 46379
tls = False
ttl = 300


async def main():
    r = await redis.RedisCluster(host=host, port=port)
    print(f"ping: {await r.ping()}")

    async with r.pipeline() as pipe:
        set_json, set_expire = await (
            pipe
            .json().set('test:test6', Jsonpath.root_path(), {'test': 'works'}, nx=True)  # nx: if key not exists
            .expire('test:test6', ttl)
            .execute()
        )
        assert set_json, "setting key failed"
        assert set_expire, "setting expire failed"
    print(f"get result: {await r.json().get('test:test6')}")
    await r.close()

asyncio.run(main())
