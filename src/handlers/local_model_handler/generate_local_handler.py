from llama_cpp import (ChatCompletionRequestSystemMessage,
                       ChatCompletionRequestUserMessage, Llama)

from schemas.answer import Answer
from schemas.task import Task
from settings import settings

system_prompt = 'Ты — помощник, который даёт краткие ответы.'


def handle_task_with_local_model(task: Task) -> Answer:
    """Handle task with local model inference"""

    # lazy model inference:
    if not hasattr(handle_task_with_local_model, 'llm'):
        handle_task_with_local_model.llm = load_model()

    system_message = ChatCompletionRequestSystemMessage(
        role='system',
        content=system_prompt)
    user_message = ChatCompletionRequestUserMessage(
        role='user',
        content=task.prompt)

    try:
        output = handle_task_with_local_model.llm.create_chat_completion(
            messages=[system_message, user_message], max_tokens=512)
    except Exception as e:
        raise RuntimeError(
            f'⚠️ Ошибка обработки с помощью локальной модели: {str(e)}')

    answer = Answer(text=output['choices'][0]['message']['content'].strip())
    return answer


def load_model() -> Llama:
    try:
        model = Llama(
            model_path=settings.MODEL_PATH,
            n_ctx=65536,
            n_thread=12,
            n_batch=512,
            verbose=False
        )
        return model
    except Exception as e:
        raise RuntimeError(
            f'⚠️ Ошибка инициализации локальной модели: {e}')
