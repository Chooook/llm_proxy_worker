import asyncio
import json

from aioredis import Redis
from loguru import logger

from settings import settings
from utils.gp_utils import run_query
from utils.redis_utils import mark_task_failed

SCHEMA = settings.GP_SCHEMA


async def _handle_generate_gp_task(task_id: str, redis: Redis):

    task_data = await redis.get(f'task:{task_id}')
    if not task_data:
        logger.warning(f'⚠️ Задача {task_id} не найдена')
        raise

    task = json.loads(task_data)
    prompt = task['prompt']
    logger.debug(f'🧠 Получен prompt: {prompt}')

    set_task_query = f"SELECT {SCHEMA}.f_ask_quest('{prompt}');"
    try:
        gp_task_id = await run_query(set_task_query)
        gp_task_id = gp_task_id[0][0]
        logger.info(f'🚀 Запрос отправлен на обработку в GP, id: {gp_task_id}')
    except Exception as e:
        logger.error(f'❌ Ошибка при выполнении запроса: {e}')
        raise

    try:
        result = ''
        get_answer_query = f"SELECT {SCHEMA}.f_get_answer_by_id({gp_task_id});"
        for _ in range(30):
            result_row = await run_query(get_answer_query)
            result = result_row[0][0]
            if any(phrase in result
                   for phrase in ['Вопрос пользователя', 'еще не обработан']):
                await asyncio.sleep(5)
                continue
            break

        if any(phrase in result
               for phrase in ['Вопрос пользователя', 'еще не обработан']):
            raise RuntimeError(f"Ошибка обработки LLM через GP")

        result = result.strip()
        logger.debug(f'🧠 Результат: {result}')

        task['status'] = 'completed'
        task['result'] = result
        await redis.setex(f'task:{task_id}', 86400, json.dumps(task))
        logger.info(f'✅ Задача {task_id} выполнена')

    except Exception as e:
        await mark_task_failed(
            redis,
            task_id,
            f"Ошибка обработки LLM: {str(e)}"
        )
        raise
