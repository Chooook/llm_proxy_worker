import asyncio
import json

from loguru import logger

from utils.gp_utils import get_task_result, set_task_to_query


async def _handle_generate_gp_task(task: dict, timeout_secs: int = 150) -> str:
    """Handle task with GP sub-queue"""

    try:
        gp_task_id = await set_task_to_query(json.dumps(task))
        logger.info(
            f'🚀 Запрос отправлен на обработку в GP, gp_id: {gp_task_id}')
    except Exception as e:
        raise RuntimeError(f'❌ Ошибка при постановке задачи в очередь GP: {e}')

    result = None
    for _ in range(timeout_secs // 5):  # 2.5 min wait by default
        try:
            result = await get_task_result(gp_task_id)
            if result is None:
                await asyncio.sleep(5)
                continue
            break
        except Exception as e:
            raise RuntimeError(
                f'❌ Ошибка при получении результата задачи из очереди GP: {e}')
    if result is None:
        raise RuntimeError(
            f'❌ Таймаут ожидания результата задачи из очереди GP истек')

    return result
