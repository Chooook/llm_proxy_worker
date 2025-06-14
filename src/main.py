import asyncio
from datetime import datetime, timezone
from typing import Callable

import aioredis
from loguru import logger

from handlers import register_handlers
from schemas.answer import Answer
from schemas.task import Task
from settings import settings
from utils.redis_utils import cleanup_dlq, mark_task_failed, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')

# TODO: rewrite to save to redis when registered, add load handlers to
#  backend at startup and delete all handlers after worker stop
# FIXME: handlers have to be same in backend and worker models,
#  in redis and frontend. Make one place to define handlers
HANDLERS = {
    'dummy':
        'handlers.dummy_handler:handle_task_dummy',
    'generate_local':
        'handlers.local_model_handler:handle_task_with_local_model',
    'generate_pm':
        'handlers.pm_handler:answer_with_rag',
    'generate_spc':
        'spc_fast.multi_agent.main:run_agent',
}


async def __main():
    task_handlers = register_handlers(HANDLERS)

    if len(task_handlers) - 1 == 0:  # -1 for dummy handler
        logger.warning('❌ Доступен только dummy обработчик!')
    elif not task_handlers:
        error_msg = '❌ Нет доступных обработчиков задач!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # TODO: replace with auto freeze to GP and move to backend lifespan:
    asyncio.create_task(cleanup_dlq(redis))
    await recover_tasks(redis)

    try:
        await __worker_loop(task_handlers)
    finally:
        await redis.close()


async def __worker_loop(task_handlers: dict[str, Callable[[Task], Answer]]):

    while True:
        try:
            task_id = await redis.brpoplpush(
                'task_queue', 'processing_queue', timeout=0)
            logger.info(f'📥 Получена задача: {task_id}')
            await __process_task(task_id, task_handlers)

        except Exception as e:
            logger.error(f'⚠️ Ошибка в worker: {e}')
            await asyncio.sleep(1)


async def __process_task(
        task_id: str, task_handlers: dict[str, Callable[[Task], Answer]]):
    """Handle task"""

    task = await __get_task(task_id)

    try:
        handler = __get_handler(task, task_handlers)

        logger.debug(f'🧠 Получен prompt: {task.prompt}')
        result = handler(task)

        task.finished_at = datetime.now(timezone.utc).isoformat()
        task.status = 'completed'

        task.result = result
        logger.debug(f'🧠 Результат: {result}')

        task_as_json = task.model_dump_json()

        await redis.setex(f'task:{task_id}', 86400, task_as_json)
        await redis.lrem('processing_queue', 1, task_id)
        logger.success(f'✅ Задача {task_id} выполнена')

    except Exception as e:
        await __handle_task_error(task_id, task, e)


async def __get_task(task_id: str) -> Task:
    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            raise KeyError('⚠️ Задача не найдена')
        task = Task.model_validate_json(task_data)
    except Exception as e:
        logger.error(f'⚠️ Ошибка при запуске задачи {task_id}: {e}')
        raise
    return task


def __get_handler(task: Task,
                  task_handlers: dict[str, Callable[[Task], Answer]]
                  ) -> Callable[[Task], Answer]:
    task_type = task.task_type
    handler = task_handlers.get(task_type)

    if not handler:
        raise RuntimeError(f'⚠️ Тип задачи {task_type} не поддерживается')
    return handler


async def __handle_task_error(task_id: str, task: Task, e: Exception):
    logger.error(f'⚠️ Ошибка обработки задачи {task_id}: {e}')
    task.retries += 1
    await redis.setex(f'task:{task_id}', 86400, task.model_dump_json())
    if task.retries > 2:
        await redis.lrem('processing_queue', 1, task_id)
        await redis.rpush('dead_letters', task_id)
        error_msg = '⚠️ Задача не выполнена, превышено количество попыток'
        await mark_task_failed(redis, task_id, error_msg)
        await asyncio.sleep(1)
    else:
        await redis.lrem('processing_queue', 1, task_id)
        await redis.rpush('task_queue', task_id)
        await asyncio.sleep(1)


if __name__ == '__main__':
    redis = aioredis.Redis.from_url(
        f'redis://{settings.HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}',
        decode_responses=True
    )
    asyncio.run(__main())
