import asyncio
from datetime import datetime, timezone
from typing import Callable

import aioredis
from loguru import logger

from handlers.handlers_init import register_handlers
from schemas.answer import Answer
from schemas.task import Task
from settings import settings
from utils.redis_utils import cleanup_dlq, mark_task_failed, recover_tasks

logger.add('worker.log', level=settings.LOGLEVEL, rotation='10 MB')

HANDLERS = {
    'dummy_handler':
        'handlers.dummy_handler:_dummy_handler',
    'generate_local':
        'handlers.generate_local_handler:_handle_generate_local_task',
# FIXME: убрать использование KNOWLEDGE_VECTOR_DATABASE и reranker в
#  generate_pm, передавать в функцию только prompt
# from rag import answer_with_rag, KNOWLEDGE_VECTOR_DATABASE, reranker
    'generate_pm':
        'rag:answer_with_rag',
    'generate_spc':
        'spc_fast.multi_agent.main:run_agent',
}


async def main():
    task_handlers = register_handlers(HANDLERS)

    if len(task_handlers) - 1 == 0:  # -1 for dummy handler
        logger.warning('❌ Доступен только dummy обработчик!')
    elif not task_handlers:
        error_msg = '❌ Нет доступных обработчиков задач!'
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    asyncio.create_task(cleanup_dlq(redis))
    try:
        await __worker_loop(task_handlers)
    finally:
        await redis.close()


async def __worker_loop(task_handlers):

    await recover_tasks(redis)

    while True:
        try:
            task_id = await redis.brpoplpush(
                'task_queue', 'processing_queue', timeout=0)
            logger.info(f'📥 Получена задача: {task_id}')
            await __process_task(task_id, task_handlers)

        except Exception as e:
            logger.error(f'⚠️ Ошибка в worker: {e}')
            await asyncio.sleep(1)


async def __process_task(task_id: str, task_handlers):
    """Handle task"""

    try:
        task_data = await redis.get(f'task:{task_id}')
        if not task_data:
            raise KeyError('⚠️ Задача не найдена')
        task: dict = json.loads(task_data)
    except Exception as e:
        logger.error(f'⚠️ Ошибка при запуске задачи {task_id}: {e}')
        raise

    try:
        handler = __get_handler(task, task_handlers)

        prompt = task['prompt']
        logger.debug(f'🧠 Получен prompt: {prompt}')

        result = handler(task)

        task['finished_at'] = datetime.now(timezone.utc).isoformat()
        task['status'] = 'completed'
        task['result'] = result
        logger.debug(f'🧠 Результат: {result}')

        await redis.setex(f'task:{task_id}', 86400, json.dumps(task))
        await redis.lrem('processing_queue', 1, task_id)
        logger.success(f'✅ Задача {task_id} выполнена')

    except Exception as e:
        logger.error(f'⚠️ Ошибка обработки задачи {task_id}: {e}')
        retries = task['retries'] = task.get('retries', 0) + 1
        await redis.setex(f'task:{task_id}', 86400, json.dumps(task))
        if retries > 2:
            await redis.lrem('processing_queue', 1, task_id)
            await redis.rpush('dead_letters', task_id)
            error_msg = '⚠️ Задача не выполнена, превышено количество попыток'
            await mark_task_failed(redis, task_id, error_msg)
            await asyncio.sleep(1)
        else:
            await redis.lrem('processing_queue', 1, task_id)
            await redis.rpush('task_queue', task_id)
            await asyncio.sleep(1)


def __get_handler(task, task_handlers):
    task_type = task['task_type']
    handler = task_handlers.get(task_type)

    if not handler:
        raise RuntimeError(f'⚠️ Тип задачи {task_type} не поддерживается')
    return handler


if __name__ == '__main__':
    redis = aioredis.Redis.from_url(
        f'redis://{settings.HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}',
        decode_responses=True
    )
    asyncio.run(main())
