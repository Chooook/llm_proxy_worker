import asyncio

from redis.asyncio import Redis
from loguru import logger


async def recover_tasks(redis: Redis):
    # can make problems if func called when worker handling this tasks
    logger.info('🔍 Восстановление незавершенных задач...')
    while True:
        task_id = await redis.rpop('processing_queue')
        if not task_id:
            break
        await redis.lpush('task_queue', task_id)
        logger.info(f'♻️ Восстановлена задача: {task_id}')


async def cleanup_dlq(redis: Redis):
    while True:
        await asyncio.sleep(3600)
        logger.info('🧹 Очистка dead_letters...')
        dlq_length = await redis.llen('dead_letters')
        if dlq_length > 50:
            tasks = await redis.lrange('dead_letters', 0, -1)
            for task_id in tasks:
                await redis.delete(f'task:{task_id}')
            await redis.delete('dead_letters')
