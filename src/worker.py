import asyncio
import json
import time

from loguru import logger
from redis.asyncio import Redis
from typing_extensions import Iterable

from handlers.handlers_manager import HandlerManager
from schemas.handler import HandlerConfig
from settings import settings


class Worker:
    def __init__(self):
        self.started = False
        self.id = f'worker:{str(time.time()).replace(".", "")}'
        # TODO add redis connection pool, use as class init param
        self.redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            socket_timeout=10,
            socket_connect_timeout=5,
            decode_responses=True
        )
        self.tasks = set()
        self.shutdown_event = asyncio.Event()
        self.handler_manager = HandlerManager()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    @property
    def __handlers_str(self) -> str:
        return self.handler_manager.handlers_json_str

    @property
    def __handlers_configs(self):
        return self.handler_manager.handlers_configs.items()

    @property
    def supported_queues(self) -> Iterable[str]:
        return [f'task_queue:{handler_id}'
                for handler_id in self.available_handlers]

    @property
    def available_handlers(self) -> Iterable[str]:
        return self.handler_manager.handlers.keys()

    async def init_handlers_manager(self):
        await self.handler_manager.start_handlers()

        if not self.available_handlers:
            error_msg = '‼️ No available task handlers!'
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        logger.info(
            f'ℹ️ Available worker handlers: {list(self.available_handlers)}')

        await self.__store_worker_to_redis()
        self.create_task(self.__heartbeat_task())
        # self.create_task(self.handler_manager.monitor_inactive_handlers)
        self.started = True

    async def __store_worker_to_redis(self):

        handlers_configs = await self.__build_configs_json()

        await self.__send_heartbeat()
        async with self.redis.pipeline() as pipe:
            await pipe.set('handlers_configs', handlers_configs)
            await pipe.setex(self.id, 60, self.__handlers_str)
            await pipe.lpush('workers', self.id)
            await pipe.execute()

        logger.info(f'ℹ️ {self.id} handlers successfully stored in Redis')

    async def __build_configs_json(self):
        raw_stored_h_configs = await self.redis.get('handlers_configs')
        if raw_stored_h_configs:
            actual_configs = {
                h_id: HandlerConfig.model_validate(config)
                for h_id, config in json.loads(raw_stored_h_configs).items()}

            for h_id, h_config in self.__handlers_configs:
                actual_configs[h_id] = h_config
        else:
            actual_configs = {h_id: config for h_id, config
                              in self.__handlers_configs}

        return json.dumps(
            {h_id: config.model_dump()
             for h_id, config in actual_configs.items()})

    async def __heartbeat_task(self):
        """Update worker alive status"""
        while not self.shutdown_event.is_set():
            try:
                await self.__send_heartbeat()
                await asyncio.sleep(30)
                logger.debug('ℹ️ Heartbeat sent')
            except Exception as e:
                logger.warning(f'⚠️ Heartbeat failed: {e}')
                break

    async def __send_heartbeat(self):
        # redis setex handler_manager handlers metadata json, create schema
        await self.redis.expire(self.id, 60)

    # FIXME не все сервисы останавливаются по cleanup, можно создавать
    #  handler_manager.cleanup задачу с помощью worker.create_task и ждать
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
