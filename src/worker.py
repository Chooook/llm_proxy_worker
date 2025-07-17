import asyncio
import time
from typing import Dict

from loguru import logger
from redis.asyncio import Redis

from handlers.handlers_manager import HandlerManager
from schemas.handler import HandlerConfig
from settings import settings


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
