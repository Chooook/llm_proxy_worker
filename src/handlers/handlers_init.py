import importlib
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


def register_handlers(
        handlers_list: list[HandlerConfig]) -> dict[str, Callable[[Task], Answer]]:
    """Verify and register handlers"""
    test_task = Task(prompt='Привет')
    verified_handlers: dict[str, Callable[[Task], Answer]] = {}
    for handler in handlers_list:
        try:
            handler_func = import_handler(handler.import_path)
            handler_func(test_task)  # test launch
            verified_handlers[handler.task_type] = handler_func
            logger.info(f'✅ Обработчик "{handler.name}" зарегистрирован')
        except Exception as e:
            logger.warning(
                f'⚠️ LLM обработчик "{handler.name}" недоступен: {e}')

    return verified_handlers
