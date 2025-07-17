import traceback
from pathlib import Path

from loguru import logger

def generate_fastapi_app(handler_dir: Path, handler_config):
        """Генерирует файл FastAPI приложения с обработкой ошибок"""
        try:  # TODO move to microservices utils
            app_code = f'''
import traceback
from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.post('/process')
async def process_request(data: dict):
    try:
        from {handler_config.interface_func_module} import (
            {handler_config.interface_func_name})
        result = {handler_config.interface_func_name}(data)
        return {{'result': result}}
    except Exception as e:
        error_detail = {{
            'error': str(e),
            'traceback': traceback.format_exc()
        }}
        raise HTTPException(
            status_code=500,
            detail=error_detail
        )

@app.get('/health')
async def health_check():
    return {{'status': 'ok'}}
'''
            app_file = handler_dir / 'handler_app.py'
            app_file.write_text(app_code)

        except Exception:
            logger.error(
                f'‼️ Error generating FastAPI app '
                f'for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            raise
