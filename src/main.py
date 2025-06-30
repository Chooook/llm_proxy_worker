import asyncio
import json
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
        self.id = f'worker:{str(time.time()).replace(".", "")}'
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
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning('⚠️ Some tasks did not finish gracefully')

        try:
            await self.redis.delete(self.id)

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
    to_remove = []
    for h_config in settings.HANDLERS:
        if h_config.handler_id not in handlers_funcs:
            to_remove.append(h_config)
    for h_config in to_remove:
        settings.HANDLERS.remove(h_config)

    if not settings.HANDLERS:
        error_msg = '‼️ No available task handlers!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        f'ℹ️ Available worker handlers:'
        f' {[h.handler_id for h in settings.HANDLERS]}')

    json_worker_handlers = json.dumps(
        [h.handler_id for h in settings.HANDLERS])
    json_stored_handlers_configs = await __get_handlers_configs(redis)

    async with redis.pipeline() as pipe:
        await pipe.set('handlers_configs', json_stored_handlers_configs)
        await pipe.setex(worker.id, 30, json_worker_handlers)
        await pipe.lpush('workers', worker.id)
        await pipe.execute()

    logger.info(f'ℹ️ {worker.id} handlers successfully stored in Redis')


async def __get_handlers_configs(redis: Redis):
    raw_stored_h_configs = await redis.get('handlers_configs')

    if raw_stored_h_configs:
        stored_h_configs = {
            h_id: HandlerConfig.model_validate(config)
            for h_id, config in json.loads(raw_stored_h_configs).items()}

        for h_config in settings.HANDLERS:
            if h_config.handler_id not in stored_h_configs:
                stored_h_configs.update({h_config.handler_id: h_config})
    else:
        stored_h_configs = {
            config.handler_id: config for config in settings.HANDLERS}

    return json.dumps(
        {h_id: conf.model_dump()
         for h_id, conf in stored_h_configs.items()})


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
    handler_id_queues = [
        f'task_queue:{handler_id}' for handler_id in handlers_funcs.keys()]
    while not worker.shutdown_event.is_set():
        try:
            source_queue, task_id = await worker.redis.brpop(
                handler_id_queues, timeout=1)
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
        handler = handlers_funcs.get(task.handler_id)

        if not handler:  # TODO move task to pending?
            raise ValueError(f'Unsupported task type: {task.handler_id}')

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
                await pipe.lpush(f'task_queue:{task.handler_id}', task_id)
                await pipe.setex(f'task:{task_id}', 86400, task_data)
                await pipe.execute()

            logger.warning(
                f'⚠️ Retry for task {task_id}'
                f' (attempt {task.retries}): {error_msg}')

    except Exception as e:
        logger.error(f'‼️ Critical task processing error {task_id}: {e}')


if __name__ == '__main__':
    asyncio.run(run_worker())
