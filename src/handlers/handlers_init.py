import os
from typing import Awaitable, Callable

from loguru import logger

from handlers.dummy_handler import _dummy_handler
from settings import settings


async def register_handlers(
) -> dict[str, Callable[[dict], Awaitable[str]]]:
    """Register available handlers"""

    task_handlers: dict[str, Callable[[dict], Awaitable[str]]] = {
        'dummy': _dummy_handler}
    logger.info('✅ Обработчик dummy зарегистрирован')

    try:
        test_task_local = {'prompt': 'test prompt'}
        from handlers.generate_local_handler import _handle_generate_local_task
        if not os.path.exists(settings.MODEL_PATH):
            raise FileNotFoundError
        await _handle_generate_local_task(test_task_local)
        task_handlers['generate_local'] = _handle_generate_local_task
        logger.info('✅ Обработчик generate-local зарегистрирован')

    except ImportError:
        logger.warning(
            '⚠️ Обработчик generate_local недоступен: '
            'зависимости не установлены')
    except FileNotFoundError:
        logger.warning(
            '⚠️ Обработчик generate_local недоступен: '
            f'модель не найдена по пути: {settings.MODEL_PATH}')
    except Exception as e:
        logger.warning(
            f'⚠️ LLM обработчик generate_local недоступен: {e}')

    try:
        from handlers.generate_gp_handler import _handle_generate_gp_task

        gp_task_types = [
            'generate_pm_test',
            'generate_spc_test',
            'generate_oapso_test',
            'search_test'
        ]
        for task_type in gp_task_types:
            test_task = {'prompt': 'test prompt', 'task_type': task_type}
            handler = task_type.replace('_test', '')
            try:
                await _handle_generate_gp_task(test_task, timeout_secs=30)
                task_handlers[handler] = _handle_generate_gp_task
                logger.info(f'✅ Обработчик {handler} зарегистрирован')
            except RuntimeError:
                raise
            except Exception as e:
                logger.warning(
                    f'⚠️ Обработчик {handler} недоступен: {e}')

    except ImportError:
        logger.warning(
            '⚠️ Обработчики на основе очереди GP недоступны: '
            'зависимости не установлены')
    except RuntimeError as e:
        logger.warning(
            f'⚠️ Обработчики на основе очереди GP недоступны: {e}')
    except Exception as e:
        logger.warning(
            f'⚠️ Обработчики на основе очереди GP недоступны: {e}')

    return task_handlers
