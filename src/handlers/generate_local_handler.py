from llama_cpp import (ChatCompletionRequestSystemMessage,
                       ChatCompletionRequestUserMessage, Llama)
from loguru import logger

from settings import settings

system_prompt = 'Ты — помощник, который даёт краткие ответы.'


async def _handle_generate_local_task(task: dict) -> str:
    """Handle task with local model inference"""

    # lazy model inference:
    if not hasattr(_handle_generate_local_task, 'llm'):
        _handle_generate_local_task.llm = await load_model()

    prompt = task['prompt']
    system_message = ChatCompletionRequestSystemMessage(
        role='system',
        content=system_prompt)
    user_message = ChatCompletionRequestUserMessage(
        role='user',
        content=prompt)

    try:
        output = _handle_generate_local_task.llm.create_chat_completion(
            messages=[system_message, user_message], max_tokens=512)
    except Exception as e:
        raise RuntimeError(
            f'⚠️ Ошибка обработки с помощью локальной модели: {str(e)}')

    return output['choices'][0]['message']['content'].strip()


async def load_model() -> Llama:
    try:
        model = Llama(
            model_path=settings.MODEL_PATH,
            n_ctx=65536,
            n_thread=12,
            n_batch=512,
            verbose=False
        )
        logger.info('✅ Локальная модель LLM инициализирована')
        return model
    except Exception as e:
        raise RuntimeError(
            f'⚠️ Ошибка инициализации локальной модели: {e}')
