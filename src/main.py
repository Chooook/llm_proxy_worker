import asyncio
import json

import aioredis
from loguru import logger

from handlers.handlers_init import register_handlers
from settings import settings
from utils.redis_utils import cleanup_dlq, mark_task_failed, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


async def main():

    if len(task_handlers) - 1 == 0:  # -1 for dummy handler
        logger.error('❌ Доступен только dummy обработчик!')
    elif not task_handlers:
        error_msg = '❌ Нет доступных обработчиков задач!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    asyncio.create_task(cleanup_dlq(redis))
    try:
        await __worker_loop()
    finally:
        await redis.close()


async def __worker_loop():

    await recover_tasks(redis)

    while True:
        try:
            task_id = await redis.brpoplpush(
                'task_queue', 'processing_queue', timeout=0)
            logger.info(f'📥 Получена задача: {task_id}')
            await __process_task(task_id)

        except Exception as e:
            logger.error(f'⚠️ Ошибка в worker: {e}')
            await asyncio.sleep(1)


async def __process_task(task_id: str):
    """Handle task"""

    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            raise RuntimeError(f'⚠️ Задача {task_id} не найдена')
        task: dict = json.loads(task_data)
        handler = await __get_handler(task, task_id)

    except Exception as e:
        logger.warning(str(e))
        await mark_task_failed(redis, task_id, str(e))
        raise

    try:
        prompt = task['prompt']
        logger.debug(f'🧠 Получен prompt: {prompt}')

        result = await handler(task)
        logger.debug(f'🧠 Результат: {result}')

        task['status'] = 'completed'
        task['result'] = result
        await redis.setex(f'task:{task_id}', 86400, json.dumps(task))
        await redis.lrem('processing_queue', 1, task_id)
        logger.info(f'✅ Задача {task_id} выполнена')

    except Exception as e:
        logger.error(f'⚠️ Ошибка обработки задачи {task_id}: {str(e)}')
        retries = await redis.hincrby(f'task:{task_id}', 'retries', 1)
        if retries > 3:
            await redis.rpush('dead_letters', task_id)
            await redis.lrem('processing_queue', 1, task_id)
            await mark_task_failed(
                redis, task_id, '⚠️ Превышено число попыток')
        else:
            await redis.lrem('processing_queue', 1, task_id)
            await redis.rpush('task_queue', task_id)
        raise


async def __get_handler(task, task_id):
    task_type = task['task_type']
    handler = task_handlers.get(task_type)

    if not handler:
        raise KeyError(f'⚠️ Задача {task_id} не выполнена, тип '
                       f'задачи {task_type} не поддерживается')
    return handler


if __name__ == '__main__':
    redis = aioredis.Redis.from_url(
        f'redis://{settings.HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}',
        decode_responses=True
    )
    task_handlers = asyncio.run(register_handlers())
    asyncio.run(main())
