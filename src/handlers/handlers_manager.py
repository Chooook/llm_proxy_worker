import asyncio
import json
import time
import traceback
from typing import Dict, Optional

from loguru import logger

from microservices.handler_service import HandlerService
from settings import settings  # TODO: remove, use as class init params


class HandlerManager:
    def __init__(self):
        self.handlers: Dict[str, HandlerService] = {}
        self.port_pool = set(range(*settings.HANDLER_PORT_RANGE))

    @property
    def handlers_configs(self):
        return {service.config_obj.handler_id: service.config_obj
                for service in self.handlers.values()}

    @property
    def handlers_json_str(self) -> str:
        return json.dumps([handler_id for handler_id in self.handlers.keys()])

    async def get_handler_process_method(self, handler_id: str):
        return self.handlers.get(handler_id).process_task

    async def start_handlers(self) -> Optional[int]:
        for handler_config in settings.HANDLERS:
            handler_id = handler_config.handler_id
            handler_service = HandlerService(handler_config)
            try:
                await handler_service.prepare_executables()
                handler_service.generate_fastapi_app()

                if not self.port_pool:
                    logger.error('‼️ No available ports for handler: '
                                 f'{handler_id}')
                handler_service.port = self.port_pool.pop()
                await handler_service.start(self.port_pool.pop())

                if not await handler_service.verify():
                    port = await handler_service.stop()
                    self.port_pool.add(port)
                    raise

                self.handlers[handler_id] = handler_service

            except Exception as e:
                self.port_pool.add(handler_service.port)
                logger.error(
                    f'‼️ Error starting handler {handler_id}: {e}')
                logger.debug(f'{traceback.format_exc()}')

    async def cleanup(self):
        for handler_service in self.handlers.values():
            try:
                await handler_service.stop()
            except Exception as e:
                logger.error(f'‼️ Error during HandlerManager cleanup: {e}')
                logger.debug(f'{traceback.format_exc()}')

    async def monitor_inactive_handlers(self):
        while True:
            try:
                await asyncio.sleep(60)
                await self._stop_inactive_handlers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f'‼️ Error in inactive handlers monitor: {e}')
                logger.debug(f'{traceback.format_exc()}')

    async def _stop_inactive_handlers(self):
        try:
            current_time = time.time()
            to_stop = []

            for handler_service in self.handlers.values():
                if handler_service.is_active:
                    time_diff = current_time - handler_service.last_active_time
                    if time_diff > settings.HANDLER_INACTIVITY_TIMEOUT:
                        to_stop.append(handler_service)

            for handler_service in to_stop:
                await handler_service.stop()

        except Exception as e:
            logger.error(f'‼️ Error stopping inactive handlers: {e}')
            logger.debug(f'{traceback.format_exc()}')
