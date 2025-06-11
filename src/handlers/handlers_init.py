import importlib
from typing import Callable

from loguru import logger


def import_handler(import_string: str) -> Callable[[dict], str]:
    """Import awaitable function by string 'module:func'"""
    module_name, func_name = import_string.split(':')
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def register_handlers(
        handlers_list: dict[str, str]) -> dict[str, Callable[[dict], str]]:
    """Verify and register handlers"""
    test_task = {'prompt': 'Привет'}
    verified_handlers: dict[str, Callable[[dict], str]] = {}
    for handler_name, handler_func in handlers_list.items():
        try:
            handler = import_handler(handler_func)
            handler(test_task)  # test launch
            verified_handlers[handler_name] = handler
            logger.info(f'✅ Обработчик {handler_name} зарегистрирован')
        except Exception as e:
            logger.warning(
                f'⚠️ LLM обработчик {handler_name} недоступен: {e}')

    return verified_handlers
