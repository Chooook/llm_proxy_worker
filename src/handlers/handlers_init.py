import importlib
import time
from typing import Callable

from loguru import logger

from schemas.answer import Answer
from schemas.task import Task
from schemas.handler import HandlerConfig


def import_handler(import_string: str) -> Callable[[Task], Answer]:
    """Import awaitable function by string 'module:func'"""
    module_name, func_name = import_string.split(':')
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def verify_handlers(
        handlers_list: list[HandlerConfig]
) -> dict[str, Callable[[Task], Answer]]:
    """Verify and register handlers"""
    test_task = Task(prompt='Привет')
    verified_handlers: dict[str, Callable[[Task], Answer]] = {}
    for handler in handlers_list:
        try:
            for attempt in range(3):
                try:
                    handler_func = import_handler(handler.import_path)
                    handler_func(test_task)  # test launch
                    verified_handlers[handler.handler_id] = handler_func
                    logger.info(
                        f'✅️ Handler "{handler.handler_id}" is ready')
                    break
                except ImportError:
                    raise
                except Exception as e:
                    if attempt == 2:
                        raise e
                    time.sleep(3)

        except ImportError as e:
            logger.warning(
                f'⚠️ Handler import error '
                f'"{handler.handler_id}": {e}')
        except Exception as e:
            logger.warning(
                f'⚠️ Handler "{handler.handler_id}" is unavailable: {e}')

    return verified_handlers
