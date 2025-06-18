import asyncio
import json
import os
import sys
import time
from typing import Callable

from redis.asyncio import Redis
from loguru import logger

from handlers import register_handlers
from schemas.answer import Answer
from schemas.task import Task
from settings import settings
from utils.redis_utils import cleanup_dlq, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


async def __main():
    worker_started = False
    worker_id = f'worker:{os.getpid()}'
    task_handlers = register_handlers(settings.HANDLERS)

    try:
        any_workers_exist = await redis.exists('worker_count')

        await __store_handlers(task_handlers)

        await redis.setex(worker_id, 30, 'alive')
        asyncio.create_task(heartbeat(worker_id))

        asyncio.create_task(cleanup_dlq(redis))

        if not any_workers_exist:
            await recover_tasks(redis)

        await __worker_loop(task_handlers)
        worker_started = True
    except Exception as e:
        logger.error(f'Worker startup failed: {e}')
        raise
    finally:
        if worker_started:
            try:
                current_workers_count = int(await redis.decr('worker_count'))
                if current_workers_count <= 0:
                    await asyncio.gather(
                        redis.delete('available_handlers'),
                        redis.delete('worker_count'),
                        redis.delete(worker_id)
                    )
                logger.info(
                    'Worker stopped. '
                    f'Current workers: {current_workers_count}')
            except Exception as e:
                logger.error(f'Cleanup error: {e}')
        await redis.close()


async def heartbeat(worker_id):
    """Update worker alive status"""
    while True:
        try:
            await redis.expire(worker_id, 30)
            await asyncio.sleep(15)
        except Exception as e:
            logger.warning(f'⚠️ Heartbeat failed: {e}')
            break


async def __store_handlers(task_handlers: dict[str, Callable[[Task], Answer]]):
    """Store and verify handlers in Redis"""
    # Фильтрация доступных обработчиков
    available_handlers = [
        h_config for h_config in settings.HANDLERS
        if h_config.task_type in task_handlers
    ]

    if not available_handlers:
        error_msg = '❌ Нет доступных обработчиков задач!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info('✅ Доступные обработчики: '
                f'{[h.task_type for h in available_handlers]}')

    handlers_data = [h.dict() for h in available_handlers]
    serialized_handlers = json.dumps(handlers_data)

    raw_stored_handlers = await redis.get('available_handlers')

    if raw_stored_handlers:
        stored_handlers = json.loads(raw_stored_handlers)
        stored_types = {h['task_type'] for h in stored_handlers}
        current_types = {h.task_type for h in available_handlers}

        if stored_types != current_types:
            missing = current_types - stored_types
            extra = stored_types - current_types
            error_msg = ('⚠️ Несоответствие обработчиков '
                         'с существующими в Redis! '
                         f'Отсутствуют: {missing}, Излишние: {extra}')
            logger.error(error_msg)
            raise ValueError(error_msg)

        await redis.incr('worker_count')
    else:
        async with redis.pipeline() as pipe:
            await (pipe.set('available_handlers', serialized_handlers)
                       .set('worker_count', 1)
                       .execute())
        logger.info('Initialized Redis with new handlers')


async def __worker_loop(task_handlers: dict[str, Callable[[Task], Answer]]):
    """Start main worker processing loop"""
    while True:
        try:
            # timeout for signals handling
            task_id = await redis.brpoplpush(
                'task_queue', 'processing_queue', timeout=1)
            if not task_id:
                continue
            logger.info(f'📥 Получена задача: {task_id}')

            await __process_task(task_id, task_handlers)

        except asyncio.CancelledError:
            logger.info('Worker loop cancelled')
            break
        except Exception as e:
            logger.error(f'⚠️ Ошибка в worker: {e}')
            await asyncio.sleep(1)


async def __process_task(
        task_id: str, task_handlers: dict[str, Callable[[Task], Answer]]):
    try:
        task = await __get_task(task_id)
        handler = task_handlers.get(task.task_type)

        if not handler:
            raise ValueError(f'⚠️ Unsupported task type: {task.task_type}')

        logger.debug(f'🧠 Получен prompt: {task.prompt}')
        start_time = time.time()
        result = handler(task)
        processing_time = time.time() - start_time

        if isinstance(result, str):
            result = Answer(text=result)

        task.status = 'completed'
        task.result = result
        task.worker_processing_time = processing_time
        logger.debug(f'🧠 Результат: {result}')

        async with redis.pipeline() as pipe:
            await (pipe.setex(f'task:{task_id}', 86400, task.model_dump_json())
                       .lrem('processing_queue', 1, task_id)
                       .execute())

        logger.success(
            f'✅ Задача {task_id} выполнена за {processing_time:.2f}s')

    except Exception as e:
        await __handle_task_error(task_id, e)


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


async def __handle_task_error(task_id: str, error: Exception):
    """Handle task processing errors"""
    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            logger.error(f'⚠️ Task {task_id} not found')
            return

        task = Task.model_validate_json(task_data)
        task.retries += 1
        error_msg = str(error)

        if task.retries >= settings.MAX_RETRIES:
            task.error = Answer(text=error_msg)
            task.status = 'failed'
            async with redis.pipeline() as pipe:
                task_data = task.model_dump_json()
                await (pipe.lrem('processing_queue', 1, task_id)
                           .rpush('dead_letters', task_id)
                           .setex(f'task:{task_id}', 86400, task_data)
                           .execute())
            logger.error(f'⚠️ Задача {task_id} перемещена в DLQ: {error_msg}')
        else:
            task_data = task.model_dump_json()
            async with redis.pipeline() as pipe:
                await (pipe.lrem('processing_queue', 1, task_id)
                           .rpush('task_queue', task_id)
                           .setex(f'task:{task_id}', 86400, task_data)
                           .execute())
            logger.warning(
                f'🔄 Повторная попытка для задачи {task_id}'
                f' (попытка {task.retries}): {error_msg}')

    except Exception as e:
        logger.error(f'⚠️ Критическая ошибка обработки задачи {task_id}: {e}')


if __name__ == '__main__':
    redis = Redis(host=settings.HOST,
                  port=settings.REDIS_PORT,
                  db=settings.REDIS_DB,
                  socket_timeout=10,
                  socket_connect_timeout=5,
                  decode_responses=True)
    try:
        asyncio.run(__main())
    except KeyboardInterrupt:
        logger.info('Worker stopped by user')
    except Exception as err:
        logger.critical(f'Worker fatal error: {err}')
        sys.exit(1)
