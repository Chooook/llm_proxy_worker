import asyncio
import json

from aioredis import Redis
from loguru import logger


async def recover_tasks(redis: Redis):
    logger.info("🔍 Восстановление незавершенных задач...")
    while True:
        task_id = await redis.rpop('processing_queue')
        if not task_id:
            break
        await redis.rpush('task_queue', task_id)
        logger.info(f"♻️ Восстановлена задача: {task_id}")


async def mark_task_failed(redis: Redis, task_id: str, error_msg: str):
    task_data = await redis.get(f'task:{task_id}')
    if task_data:
        task = json.loads(task_data)
        task['status'] = 'failed'
        task['error'] = error_msg
        await redis.setex(f'task:{task_id}', 86400, json.dumps(task))


async def cleanup_dlq(redis: Redis):
    while True:
        await asyncio.sleep(3600)
        logger.info("🧹 Очистка dead_letters...")
        dlq_length = await redis.llen('dead_letters')
        if dlq_length > 50:
            tasks = await redis.lrange('dead_letters', 0, -1)
            for task_id in tasks:
                await redis.delete(f'task:{task_id}')
            await redis.delete('dead_letters')
        await asyncio.sleep(3600)
