import asyncio
from datetime import datetime, timezone

from redis.asyncio import Redis
from loguru import logger

from schemas.answer import Answer
from schemas.task import Task


async def recover_tasks(redis: Redis):
    # FIXME: can make problems if func called when worker handling this tasks
    logger.info('🔍 Восстановление незавершенных задач...')
    while True:
        task_id = await redis.rpop('processing_queue')
        if not task_id:
            break
        await redis.lpush('task_queue', task_id)
        logger.info(f'♻️ Восстановлена задача: {task_id}')


async def mark_task_failed(redis: Redis, task_id: str, error_msg: str):
    task_data = await redis.get(f'task:{task_id}')
    if task_data:
        task = Task.model_validate_json(task_data)
        task.status = 'failed'
        task.finished_at = datetime.now(timezone.utc).isoformat()
        task.error = Answer(text=error_msg)
        await redis.setex(f'task:{task_id}', 86400, task.model_dump_json())


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
