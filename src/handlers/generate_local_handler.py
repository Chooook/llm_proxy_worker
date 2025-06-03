import json

from aioredis import Redis
from loguru import logger

from settings import settings
from utils.redis_utils import mark_task_failed


async def _handle_generate_local_task(task_id: str, redis: Redis):
    """Обработчик для задач с локальным инференсом"""
    try:
        from llama_cpp import (Llama,
                               ChatCompletionRequestUserMessage,
                               ChatCompletionRequestSystemMessage)
    except ImportError:
        await mark_task_failed(
            redis,
            task_id,
            "LLM обработка недоступна: отсутствуют зависимости"
        )
        raise RuntimeError("LLM зависимости не установлены")

    task_data = await redis.get(f'task:{task_id}')
    if not task_data:
        logger.warning(f'⚠️ Задача {task_id} не найдена')
        await mark_task_failed(
            redis,
            task_id,
            "Задача не найдена"
        )
        raise RuntimeError("Задача не найдена")

    # Ленивая инициализация модели
    if not hasattr(_handle_generate_local_task, 'llm'):
        try:
            _handle_generate_local_task.llm = Llama(
                model_path=settings.MODEL_PATH,
                n_ctx=65536,
                n_thread=12,
                n_batch=512,
                verbose=False
            )
            logger.info('✅ Модель LLM инициализирована')
        except Exception as e:
            await mark_task_failed(
                redis,
                task_id,
                f"Ошибка инициализации модели: {str(e)}"
            )
            raise RuntimeError(f"Ошибка модели: {e}")

    task = json.loads(task_data)
    prompt = task['prompt']
    logger.debug(f'🧠 Получен prompt: {prompt}')

    try:
        system_message: ChatCompletionRequestSystemMessage = {
            "role": "system",
            "content": "Ты — помощник, который даёт краткие ответы.",
        }
        user_message = ChatCompletionRequestUserMessage(
            role="user",
            content=prompt
        )
        output = _handle_generate_local_task.llm.create_chat_completion(
            messages=[system_message, user_message],
            max_tokens=512,
        )
        result = output['choices'][0]['message']['content'].strip()
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
