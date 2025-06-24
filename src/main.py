import asyncio
import json
import os
import signal
import sys
import time
from typing import Callable

from loguru import logger
from redis.asyncio import Redis

from handlers import verify_handlers
from schemas.answer import Answer
from schemas.handler import HandlerConfig
from schemas.task import Task, TaskStatus
from settings import settings

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
            logger.warning('⚠️ Some tasks did not finish gracefully')

        try:
            await self.redis.delete(self.id)
            current_workers = int(await self.redis.decr('worker_count'))
            if current_workers <= 0:
                redis_handlers = [
                    HandlerConfig.model_validate(h)
                    for h in json.loads(await self.redis.get('handlers'))]
                for handler in redis_handlers:
                    handler.available_workers = 0
                serialized_handlers = json.dumps(
                    [h.model_dump() for h in redis_handlers])
                await self.redis.set('handlers', serialized_handlers)

            else:
                # TODO: rework with set every handler standalone
                #  so we can decrement them separately (handler:type:version)
                redis_handlers = [
                    HandlerConfig.model_validate(h)
                    for h in json.loads(await self.redis.get('handlers'))]
                types_to_decrement = {
                    (h.task_type, h.version)
                    for h in settings.HANDLERS if h.available_workers}
                for handler in types_to_decrement:
                    for h in redis_handlers:
                        if (h.task_type == handler[0]
                                and h.version == handler[1]):
                            h.available_workers -= 1
                handlers_data = [h.model_dump() for h in redis_handlers]
                serialized_handlers = json.dumps(handlers_data)
                await self.redis.set('handlers', serialized_handlers)

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

        try:
            handlers_funcs = verify_handlers(settings.HANDLERS)

            await __store_handlers(worker, handlers_funcs)
            worker.started = True

            await worker.redis.setex(worker.id, 30, 'alive')
            worker.create_task(heartbeat(worker))

            await __worker_loop(worker, handlers_funcs)

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker stopped gracefully')
        except Exception as e:
            logger.critical(f'‼️ Worker crashed: {e}')
            raise


async def __store_handlers(
        worker:Worker, handlers_funcs: dict[str, Callable[[Task], Answer]]):
    redis = worker.redis
    """Store and verify handlers in Redis"""
    # FIXME probably bad with versioning handlers:
    for h_config in settings.HANDLERS:
        if h_config.task_type in handlers_funcs:
            h_config.available_workers += 1

    if all([not h.available_workers for h in settings.HANDLERS]):
        error_msg = '‼️ No available task handlers!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        'ℹ️ Available handlers: '
        f'{[h.task_type for h in settings.HANDLERS if h.available_workers]}')

    raw_stored_handlers = await redis.get('handlers')

    if raw_stored_handlers:
        stored_handlers = [HandlerConfig.model_validate(h)
                           for h in json.loads(raw_stored_handlers)]
        stored_versions = [
            f'{h.task_type}:{h.version}' for h in stored_handlers]

        for h_config in settings.HANDLERS:
            if h_config.available_workers == 0:
                continue

            h_type_with_version = f'{h_config.task_type}:{h_config.version}'
            if h_type_with_version not in stored_versions:
                continue

            same_stored_handler = [h for h in stored_handlers
                                   if h.task_type == h_config.task_type
                                   and h.version == h_config.version][0]
            h_config.available_workers += same_stored_handler.available_workers

            logger.info(f'ℹ️ Updated workers count for '
                        f'{h_config.task_type}:{h_config.version}')

        settings_config_versions = {
            f'{h.task_type}:{h.version}' for h in stored_handlers}
        for h_config, h_version in zip(stored_handlers, stored_versions):
            if (h_config.task_type
                    not in [h.task_type for h in settings.HANDLERS]
                    or h_version not in settings_config_versions):
                settings.HANDLERS.append(h_config)

    handlers_data = [h.model_dump() for h in settings.HANDLERS]
    serialized_handlers = json.dumps(handlers_data)

    async with redis.pipeline() as pipe:
        if raw_stored_handlers:
            await pipe.incr('worker_count')
        else:
            await pipe.set('worker_count', 1)
        await pipe.set('handlers', serialized_handlers)
        await pipe.execute()

    logger.info(f'ℹ️ {worker.id} handlers successfully stored in Redis')


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
        worker: Worker, handlers_funcs: dict[str, Callable[[Task], Answer]]):
    """Start main worker processing loop"""
    task_type_queues = [
        f'task_queue:{task_type}' for task_type in handlers_funcs.keys()]
    while not worker.shutdown_event.is_set():
        try:
            source_queue, task_id = await worker.redis.brpop(
                task_type_queues, timeout=1)
            if not task_id:
                continue

            logger.info(f'ℹ️ Received task: {task_id} from {source_queue}')
            async with worker.redis.pipeline() as pipe:
                await pipe.lrem('task_queue', 0, task_id)
                await pipe.lpush('processing_queue', task_id)
                await pipe.execute()

            await __process_task(worker.redis, task_id, handlers_funcs)

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


async def __process_task(
        redis: Redis,
        task_id: str,
        handlers_funcs: dict[str, Callable[[Task], Answer]]):
    try:
        task = await __get_task(redis, task_id)
        handler = handlers_funcs.get(task.task_type)
        handler_config = [h for h in settings.HANDLERS
                          if h.task_type == task.task_type][0]
        task.task_type_version = handler_config.version

        if not handler:  # TODO move task to pending?
            raise ValueError(f'Unsupported task type: {task.task_type}')

        logger.debug(f'⚙️ Processing prompt: {task.prompt}')
        start_time = time.time()
        result = handler(task)
        processing_time = time.time() - start_time

        if isinstance(result, str):
            result = Answer(text=result)

        task.status = TaskStatus.COMPLETED
        task.result = result
        task.worker_processing_time = processing_time
        logger.debug(f'⚙️ Result: {result}')

        async with redis.pipeline() as pipe:
            await pipe.setex(f'task:{task_id}', 86400, task.model_dump_json())
            await pipe.lrem('processing_queue', 1, task_id)
            await pipe.execute()

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
                await pipe.setex(f'task:{task_id}', 86400, task_data)
                await pipe.execute()

            logger.error(f'‼️ Task {task_id} moved to DLQ: {error_msg}')
        else:
            task_data = task.model_dump_json()
            async with redis.pipeline() as pipe:
                await pipe.lrem('processing_queue', 1, task_id)
                await pipe.rpush('task_queue', task_id)
                await pipe.lpush(f'task_queue:{task.task_type}', task_id)
                await pipe.setex(f'task:{task_id}', 86400, task_data)
                await pipe.execute()

            logger.warning(
                f'⚠️ Retry for task {task_id}'
                f' (attempt {task.retries}): {error_msg}')

    except Exception as e:
        logger.error(f'‼️ Critical task processing error {task_id}: {e}')


if __name__ == '__main__':
    asyncio.run(run_worker())
