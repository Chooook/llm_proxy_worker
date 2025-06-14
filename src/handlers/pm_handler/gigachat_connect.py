import json
import os
import time
import warnings

import requests

warnings.filterwarnings('ignore')

os.environ['TOKENIZERS_PARALLELISM'] = 'true'
GIGACHAT_API_COMPLETIONS_URL = 'http://liveaccess/v1/gc/chat/completions'
HEADERS = {'Authorization': f'Bearer {os.environ.get("JPY_API_TOKEN")}',
           'Content-Type': 'application/json'}


def throttle(seconds=5):
    """Decorator for throttling function calls."""
    last_called = 0

    def decorator(func):
        def wrapper(*args, **kwargs):
            nonlocal last_called
            current_time = time.time()
            if current_time - last_called < seconds:
                return None
            last_called = current_time
            return func(*args, **kwargs)
        return wrapper
    return decorator


@throttle()
def get_answer(prompt):
    """Get answer from GigaChat."""
    answer = json.loads(completions(prompt))
    return answer['choices'][0]['message']['content']


def completions(prompt: str):
    data = {'model': 'GigaChat-2-Max', 'n': 1, 'temperature': 0.01,
            'messages': [{'role': 'user', 'content': str(prompt)}]}
    response = requests.post(
        url=GIGACHAT_API_COMPLETIONS_URL, headers=HEADERS, json=data)

    if response.ok:
        return json.dumps(response.json(), indent=4, ensure_ascii=False)
    else:
        return response.text
