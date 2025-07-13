import asyncio
import json
import os
import signal
import sys
import time
from typing import Dict

import httpx
from loguru import logger
from redis.asyncio import Redis

from handlers.handlers_manager import HandlerManager
from schemas.answer import Answer
from schemas.handler import HandlerConfig
from schemas.task import Task, TaskStatus
from settings import settings

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')


class Worker:
    def __init__(self):
        self.started = False
        self.id = f'worker:{str(time.time()).replace(".", "")}'
        self.redis = Redis(  # TODO add redis connection pool
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            socket_timeout=10,
            socket_connect_timeout=5,
            decode_responses=True
        )
        self.tasks = set()
        self.shutdown_event = asyncio.Event()
        self.handlers = ''
        self.handler_manager = HandlerManager()
        self.valid_handlers: Dict[str, HandlerConfig] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def setup_handlers(self):
        await self.redis.setex(self.id, 120, self.handlers)

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
            await self.redis.lrem('workers', 0, self.id)
            await self.handler_manager.cleanup()
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
    git_login = os.getenv('GIT_LOGIN')
    git_pass = os.getenv('GIT_PASS')
    if not any([git_login, git_pass]):
        logger.warning(
            '⚠️ Git credentials not set in env, repository access may fail')

    async with Worker() as worker:
        if sys.platform != 'win32':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, worker.shutdown_event.set)

        try:
            worker.create_task(
                worker.handler_manager.monitor_inactive_handlers())

            await __store_handlers(worker)
            worker.started = True
            worker.create_task(heartbeat(worker))

            await __worker_loop(worker)

        except asyncio.CancelledError:
            logger.info('ℹ️ Worker stopped gracefully')
        except Exception as e:
            logger.critical(f'‼️ Worker crashed: {e}')
            raise


async def __store_handlers(worker: Worker):
    redis = worker.redis
    """Store and verify handlers in Redis"""
    to_remove = []
    valid_handlers = {}

    for h_config in settings.HANDLERS:
        # Проверяем работоспособность обработчика
        if await worker.handler_manager.verify_handler(h_config.handler_id):
            valid_handlers[h_config.handler_id] = h_config
        else:
            logger.error('‼️ Handler validation failed for '
                         f'{h_config.handler_id}')
            to_remove.append(h_config)

    # Удаляем нерабочие обработчики
    for h_config in to_remove:
        settings.HANDLERS.remove(h_config)

    if not settings.HANDLERS:
        error_msg = '‼️ No available task handlers!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        f'ℹ️ Available worker handlers:'
        f' {[h.handler_id for h in settings.HANDLERS]}')

    worker.handlers = json.dumps([h.handler_id for h in settings.HANDLERS])
    worker.valid_handlers = valid_handlers

    json_stored_handlers_configs = await __get_handlers_configs(redis)

    await worker.setup_handlers()
    async with redis.pipeline() as pipe:
        await pipe.set('handlers_configs', json_stored_handlers_configs)
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
            await worker.setup_handlers()
            await asyncio.sleep(30)
            logger.debug('ℹ️ Heartbeat sent')
        except Exception as e:
            logger.warning(f'⚠️ Heartbeat failed: {e}')
            break


async def __worker_loop(worker: Worker):
    """Start main worker processing loop"""
    # Семафор для ограничения количества одновременно выполняемых задач
    concurrency_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_TASKS)

    # Формируем очереди только для действительных обработчиков
    handler_id_queues = [
        f'task_queue:{handler_id}'
        for handler_id in worker.valid_handlers.keys()
    ]

    while not worker.shutdown_event.is_set():
        try:
            result = await worker.redis.brpop(handler_id_queues, timeout=1)
            if not result:
                continue

            source_queue, task_id = result
            # Извлекаем handler_id из имени очереди
            handler_id = source_queue.split(':', 1)[1]

            logger.info(
                f'ℹ️ Received task: {task_id} for handler {handler_id}')
            async with worker.redis.pipeline() as pipe:
                await pipe.lrem('task_queue', 0, task_id)
                await pipe.lpush('processing_queue', task_id)
                await pipe.execute()

            # Запускаем обработку задачи в отдельной асинхронной задаче
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
        url = await handler_manager.get_handler_url(handler_id)
        if not url:
            raise Exception(f'Handler {handler_id} is not available')

        # Отправляем запрос в FastAPI обработчик
        # TODO сделать один общий клиент в Worker
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(url, json=task.model_dump())
            if response.status_code != 200:
                raise Exception(
                    f'Handler error: {response.status_code} - {response.text}')

            result_data = response.json()['result']

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
