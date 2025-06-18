import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Callable

from redis.asyncio import Redis
from loguru import logger

from handlers import register_handlers
from schemas.answer import Answer
from schemas.handler import HandlerConfig
from schemas.task import Task
from settings import settings
from utils.redis_utils import cleanup_dlq, mark_task_failed, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


async def __main():
    worker_started = False
    task_handlers = register_handlers(settings.HANDLERS)

    if len(task_handlers) - 1 == 0:  # -1 for dummy handler
        logger.warning('❌ Доступен только dummy обработчик!')
    elif not task_handlers:
        error_msg = '❌ Нет доступных обработчиков задач!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        await __store_handlers(task_handlers)
        asyncio.create_task(cleanup_dlq(redis))

        await recover_tasks(redis)
        await __worker_loop(task_handlers)
        worker_started = True
    finally:
        if worker_started:
            current_count = int(await redis.decr("worker_count"))
            if current_count <= 0:
                await redis.delete("available_handlers")
                await redis.delete("worker_count")
                print('Завершение работы...', flush=True)
        await redis.close()


async def __store_handlers(task_handlers: dict[str, Callable[[Task], Answer]]):
    """Store handlers in redis"""
    verified_handlers_configs = [
        h_config for h_config in settings.HANDLERS
        if h_config.task_type in task_handlers.keys()]
    logger.info(f'✅ Доступные обработчики: {verified_handlers_configs}')

    raw_stored_handlers = await redis.get('available_handlers')
    json.dumps([h_config.dict() for h_config in verified_handlers_configs])

    if raw_stored_handlers:
        stored_handlers = [
            HandlerConfig.model_validate(h_config)
            for h_config in json.loads(raw_stored_handlers)]

        stored_types = {h.task_type for h in stored_handlers}
        verified_types = {h_type for h_type in task_handlers.keys()}

        if stored_types != verified_types:
            missing = verified_types - stored_types
            extra = stored_types - verified_types
            raise ValueError(f'⚠️ Несоответствующие обработчики '
                             f'с существующими в redis! '
                             f'Отсутствуют: {missing}, '
                             f'Излишние: {extra}')

        await redis.incr('worker_count')
    else:
        configs_json_dump = json.dumps(
            [h_config.dict() for h_config in verified_handlers_configs])
        await redis.set('available_handlers', configs_json_dump)
        await redis.set('worker_count', 1)


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
        result: Answer | str = handler(task)
        if isinstance(result, str):
            result = Answer(text=result)

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
    redis = Redis(host=settings.HOST,
                  port=settings.REDIS_PORT,
                  db=settings.REDIS_DB,
                  decode_responses=True)
    try:
        asyncio.run(__main())
    except KeyboardInterrupt:
        logger.info('Worker stopped by user')
    except Exception as e:
        logger.critical(f'Worker fatal error: {e}')
        sys.exit(1)
