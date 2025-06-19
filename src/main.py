import asyncio
import asyncio_atexit
import json
import os
import sys
import signal
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


class Worker:
    def __init__(self):
        self.started = False
        self.id = f'worker:{os.getpid()}'
        self.redis = Redis(
            host=settings.HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            socket_timeout=10,
            socket_connect_timeout=5,
            decode_responses=True
        )
        self.tasks = set()
        self.shutdown_event = asyncio.Event()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def cleanup(self):
        if not self.started:
            logger.info('ℹ️ Worker was not started, skipping cleanup')
            return

        logger.info('ℹ️ Starting cleanup procedure...')

        for task in self.tasks:
            task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(*self.tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning('⚠️ Some tasks didn`t finish gracefully')

        try:
            current_workers = int(await self.redis.decr('worker_count'))
            if current_workers <= 0:
                await asyncio.gather(
                    self.redis.delete('available_handlers'),
                    self.redis.delete('worker_count'),
                    self.redis.delete(self.id)
                )
            logger.info(f'⚒️ Remaining workers: {current_workers}')
        except Exception as e:
            logger.error(f'‼️ Cleanup error: {e}')
        finally:
            await self.redis.aclose()
            logger.success('✅️ Worker shutdown completed')

    def create_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(lambda t: self.tasks.remove(t))
        return task


async def run_worker():
    async with Worker() as worker:
        if sys.platform != 'win32':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, worker.shutdown_event.set)

        asyncio_atexit.register(worker.cleanup)

        try:
            task_handlers = register_handlers(settings.HANDLERS)
            any_workers_exist = await worker.redis.exists('worker_count')

            await __store_handlers(worker.redis, task_handlers)
            worker.started = True

            await worker.redis.setex(worker.id, 30, 'alive')
            worker.create_task(heartbeat(worker))
            worker.create_task(cleanup_dlq(worker.redis))

            if not any_workers_exist:
                await recover_tasks(worker.redis)

            await __worker_loop(worker, task_handlers)

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker stopped gracefully')
        except Exception as e:
            logger.critical(f'‼️ Worker crashed: {e}')
            raise


async def __store_handlers(
        redis: Redis, task_handlers: dict[str, Callable[[Task], Answer]]):
    """Store and verify handlers in Redis"""
    available_handlers = [
        h_config for h_config in settings.HANDLERS
        if h_config.task_type in task_handlers
    ]

    if not available_handlers:
        error_msg = '‼️ No available task handlers!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info('ℹ️ Available handlers: '
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
            error_msg = ('‼️ Handlers mismatch with Redis! '
                         f'Missing: {missing}, Extra: {extra}')
            logger.error(error_msg)
            raise ValueError(error_msg)

        await redis.incr('worker_count')
    else:
        async with redis.pipeline() as pipe:
            await (pipe.set('available_handlers', serialized_handlers)
                       .set('worker_count', 1)
                       .execute())
        logger.info('ℹ️ Initialized Redis with new handlers')


async def heartbeat(worker: Worker):
    """Update worker alive status"""
    while not worker.shutdown_event.is_set():
        try:
            await worker.redis.expire(worker.id, 30)
            await asyncio.sleep(15)
        except Exception as e:
            logger.warning(f'⚠️ Heartbeat failed: {e}')
            break


async def __worker_loop(
        worker: Worker, task_handlers: dict[str, Callable[[Task], Answer]]):
    """Start main worker processing loop"""
    while not worker.shutdown_event.is_set():
        try:
            task_id = await worker.redis.brpoplpush(
                'task_queue', 'processing_queue', timeout=1)
            if not task_id:
                continue
            logger.info(f'ℹ️ Received task: {task_id}')

            await __process_task(worker.redis, task_id, task_handlers)

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker loop cancelled')
            break
        except Exception as e:
            logger.error(f'‼️ Worker error: {e}')
            await asyncio.sleep(1)


async def __process_task(
        redis: Redis,
        task_id: str,
        task_handlers: dict[str, Callable[[Task], Answer]]):
    try:
        task = await __get_task(redis, task_id)
        handler = task_handlers.get(task.task_type)

        if not handler:
            raise ValueError(f'Unsupported task type: {task.task_type}')

        logger.debug(f'⚙️ Processing prompt: {task.prompt}')
        start_time = time.time()
        result = handler(task)
        processing_time = time.time() - start_time

        if isinstance(result, str):
            result = Answer(text=result)

        task.status = 'completed'
        task.result = result
        task.worker_processing_time = processing_time
        logger.debug(f'⚙️ Result: {result}')

        async with redis.pipeline() as pipe:
            await (pipe.setex(f'task:{task_id}', 86400, task.model_dump_json())
                       .lrem('processing_queue', 1, task_id)
                       .execute())

        logger.success(
            f'✅️ Task {task_id} completed in {processing_time:.2f}s')

    except Exception as e:
        await __handle_task_error(redis, task_id, e)


async def __get_task(redis: Redis, task_id: str) -> Task:
    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            raise KeyError('Task not found')
        task = Task.model_validate_json(task_data)
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
            task.status = 'failed'
            async with redis.pipeline() as pipe:
                task_data = task.model_dump_json()
                await (pipe.lrem('processing_queue', 1, task_id)
                           .rpush('dead_letters', task_id)
                           .setex(f'task:{task_id}', 86400, task_data)
                           .execute())
            logger.error(f'‼️ Task {task_id} moved to DLQ: {error_msg}')
        else:
            task_data = task.model_dump_json()
            async with redis.pipeline() as pipe:
                await (pipe.lrem('processing_queue', 1, task_id)
                           .rpush('task_queue', task_id)
                           .setex(f'task:{task_id}', 86400, task_data)
                           .execute())
            logger.warning(
                f'⚠️ Retry for task {task_id}'
                f' (attempt {task.retries}): {error_msg}')

    except Exception as e:
        logger.error(f'‼️ Critical task processing error {task_id}: {e}')


if __name__ == '__main__':
    asyncio.run(run_worker())
