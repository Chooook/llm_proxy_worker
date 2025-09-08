import asyncio
import os
import signal
import sys
import time

import httpx
from loguru import logger
from redis.asyncio import Redis

from handlers.handlers_manager import HandlerManager
from schemas.answer import Answer
from schemas.task import Task, TaskStatus
from settings import settings
from worker import Worker

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


async def run_worker():
    if not os.getenv('GIT_TOKEN'):
        logger.info(
            '⚠️ Git token not set in env, repository access may fail')

    async with Worker() as worker:
        if sys.platform != 'win32':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, worker.shutdown_event.set)

        try:
            await worker.init_handlers_manager()
            await __worker_loop(worker)

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker stopped gracefully')
        except Exception as e:
            logger.critical(f'‼️ Worker crashed: {e}')
            raise


async def __worker_loop(worker: Worker):
    """Start main worker processing loop"""
    # semaphore for limiting the number of concurrent tasks
    concurrency_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_TASKS)

    while not worker.shutdown_event.is_set():
        try:
            # use only available handlers queues
            task = await worker.redis.brpop(
                worker.supported_queues, timeout=1)
            if not task:
                continue

            source_queue, task_id = task
            # get handler_id from queue name
            handler_id = source_queue.split(':', 1)[1]

            logger.info(
                f'ℹ️ Received task: {task_id} for handler {handler_id}')
            async with worker.redis.pipeline() as pipe:
                await pipe.lrem('task_queue', 0, task_id)
                await pipe.lpush('processing_queue', task_id)
                await pipe.execute()

            # start task processing in separate async coroutine
            worker.create_task(
                __process_task_with_semaphore(
                    concurrency_semaphore,
                    worker.redis,
                    task_id,
                    worker.handler_manager,
                    handler_id
                )
            )

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker loop cancelled')
            break
        except asyncio.TimeoutError:
            await asyncio.sleep(1)
        except TypeError:
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f'‼️ Worker error: {e}')
            await asyncio.sleep(1)


async def __process_task_with_semaphore(
        semaphore: asyncio.Semaphore,
        redis: Redis,
        task_id: str,
        handler_manager: HandlerManager,
        handler_id: str
):
    """Обрабатывает задачу с ограничением параллелизма"""
    async with semaphore:
        await __process_task(redis, task_id, handler_manager, handler_id)


async def __process_task(
        redis: Redis,
        task_id: str,
        handler_manager: HandlerManager,
        handler_id: str  # Явно передаем handler_id
):
    try:
        task = await __get_task(redis, task_id)
        start_time = time.time()

        # Получаем URL обработчика (запускает при необходимости)
        meth = await handler_manager.get_handler_process_method(handler_id)
        if not meth:
            raise Exception(f'Handler {handler_id} is not available')

        result_data = await meth(task)

        # Обрабатываем результат
        if isinstance(result_data, str):
            result = Answer(text=result_data)
        elif isinstance(result_data, dict):
            result = Answer.model_validate(result_data)
        else:
            raise TypeError(f'Unexpected result type: {type(result_data)}')

        task.status = TaskStatus.COMPLETED
        task.result = result
        task.worker_processing_time = time.time() - start_time

        async with redis.pipeline() as pipe:
            await pipe.setex(
                f'task:{task_id}',
                settings.redis_store_seconds,
                task.model_dump_json())
            await pipe.lrem('processing_queue', 1, task_id)
            await pipe.execute()

        logger.success(
            f'✅️ Task {task_id} completed '
            f'in {task.worker_processing_time:.2f}s')

    except Exception as e:
        await __handle_task_error(redis, task_id, e)


async def __get_task(redis: Redis, task_id: str) -> Task:
    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            raise KeyError('Task not found')
        task = Task.model_validate_json(task_data)
        task.status = TaskStatus.RUNNING
    except Exception as e:
        logger.error(f'‼️ Task startup error {task_id}: {e}')
        raise
    return task


async def __handle_task_error(redis: Redis, task_id: str, error: Exception):
    """Handle task processing errors"""
    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            logger.error(f'‼️ Task {task_id} not found')
            return

        task = Task.model_validate_json(task_data)
        task.retries += 1
        error_msg = str(error)

        if task.retries >= settings.MAX_RETRIES:
            task.error = Answer(text=error_msg)
            task.status = TaskStatus.FAILED
            task_data = task.model_dump_json()
            async with redis.pipeline() as pipe:
                await pipe.lrem('processing_queue', 1, task_id)
                await pipe.rpush('dead_letters', task_id)
                await pipe.setex(
                    f'task:{task_id}',
                    settings.redis_store_seconds,
                    task_data)
                await pipe.execute()

            logger.error(f'‼️ Task {task_id} moved to DLQ: {error_msg}')
        else:
            task_data = task.model_dump_json()
            async with redis.pipeline() as pipe:
                await pipe.lrem('processing_queue', 1, task_id)
                await pipe.rpush('task_queue', task_id)
                await pipe.lpush(f'task_queue:{task.handler_id}', task_id)
                await pipe.setex(
                    f'task:{task_id}',
                    settings.redis_store_seconds,
                    task_data)
                await pipe.execute()

            logger.warning(
                f'⚠️ Retry for task {task_id}'
                f' (attempt {task.retries}): {error_msg}')

    except Exception as e:
        logger.error(f'‼️ Critical task processing error {task_id}: {e}')


if __name__ == '__main__':
    asyncio.run(run_worker())
