from datetime import datetime
import redis


async def redis_server_time(client: redis.Redis):
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
