import os
from typing import Awaitable, Callable, Dict

from aioredis import Redis
from loguru import logger

from handlers.generate_gp_handler import _handle_generate_gp_task
from handlers.generate_local_handler import _handle_generate_local_task
from settings import settings


async def register_handlers(redis: Redis
        ) -> Dict[str, Callable[[str, Redis], Awaitable[None]]]:
    """Регистрирует доступные обработчики задач"""
    task_handlers: Dict[str, Callable[[str, Redis], Awaitable[None]]] = {}

    try:
        import llama_cpp
        if not os.path.exists(settings.MODEL_PATH):
            raise FileNotFoundError
        # TODO: добавить шаблонную задачу для проверки работы
        # await _handle_generate_local_task('test prompt', redis)
        task_handlers['generate_local'] = _handle_generate_local_task
        logger.info("✅ Обработчик generate-local зарегистрирован")
    except ImportError:
        logger.warning(
            "⚠️ Обработчик generate_local недоступен: "
            "зависимости не установлены")
    except FileNotFoundError:
        logger.warning(
            "⚠️ Обработчик generate_local недоступен: "
            f"модель не найдена по пути: {settings.MODEL_PATH}")
    except Exception as e:
        logger.warning(
            f"⚠️ LLM обработчик generate_local недоступен: {e}")

    try:
        import asyncpg
        # TODO: добавить шаблонную задачу для проверки работы
        # await _handle_generate_gp_task('test prompt', redis)
        task_handlers['generate_gp'] = _handle_generate_gp_task
        logger.info("✅ Обработчик generate_gp зарегистрирован")
    except ImportError:
        logger.warning(
            "⚠️ LLM обработчик generate_gp недоступен: "
            "зависимости не установлены")
    except RuntimeError:
        logger.warning(
            "⚠️ LLM обработчик generate_gp недоступен: "
            "тикет kerberos недействителен")
    except Exception as e:
        logger.warning(
            f"⚠️ LLM обработчик generate_gp недоступен: {e}")


    return task_handlers
