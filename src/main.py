import asyncio
import json

import aioredis
from loguru import logger

from handlers.handlers_init import register_handlers
from settings import settings
from utils.redis_utils import cleanup_dlq, mark_task_failed, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


async def main(redis):

    if not task_handlers:
        logger.error("❌ Нет доступных обработчиков задач!")
        return

    asyncio.create_task(cleanup_dlq(redis))
    try:
        await worker_loop(redis, task_handlers)
    finally:
        await redis.close()


async def worker_loop(redis: aioredis.Redis, handlers: dict):
    await recover_tasks(redis)

    while True:
        try:
            task_id = await redis.brpoplpush(
                'task_queue',
                'processing_queue',
                timeout=0
            )
            logger.info(f'📥 Получена задача: {task_id}')

            try:
                await process_task(task_id, redis, handlers)
                await redis.lrem('processing_queue', 1, task_id)
            except Exception:
                pass

        except Exception as e:
            logger.error(f'⚠️ Ошибка в worker: {e}')
            await asyncio.sleep(1)


async def process_task(
        task_id: str, redis: aioredis.Redis, handlers: dict):
    """Основной обработчик задач"""
    task_data = await redis.get(f'task:{task_id}')
    if not task_data:
        logger.warning(f'⚠️ Задача {task_id} не найдена')
        return

    task = json.loads(task_data)
    handler = handlers.get(task['type'])

    if not handler:
        await mark_task_failed(
            redis,
            task_id,
            f"Неподдерживаемый тип задачи: {task['type']}"
        )
        logger.warning(f'⚠️ Задача {task_id} не выполнена, тип '
                       f'задачи {task["type"]} не поддерживается')
        return

    try:
        await handler(task_id, redis)
    except Exception as e:
        logger.error(f"⚠️ Ошибка обработки задачи {task_id}: {str(e)}")
        retries = await redis.hincrby(f'task:{task_id}', 'retries', 1)
        if retries > 2:
            await redis.rpush('dead_letters', task_id)
            await redis.lrem('processing_queue', 1, task_id)
            await mark_task_failed(redis, task_id, "Превышено число попыток")
        else:
            await redis.lrem('processing_queue', 1, task_id)
            await redis.rpush('task_queue', task_id)
        raise


if __name__ == '__main__':
    redis_client = aioredis.Redis.from_url(
        f'redis://{settings.HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}',
        decode_responses=True
    )
    task_handlers = asyncio.run(register_handlers(redis_client))
    asyncio.run(main(redis_client))
