import traceback
from pathlib import Path
from string import Template

from loguru import logger

FASTAPI_HANDLER_TEMPLATE = Template('''
    import traceback
    from fastapi import FastAPI, HTTPException

    app = FastAPI()

    @app.post('/process')
    async def process_request(data: dict):
        try:
            from $module import $function
            result = $function(data)
            return {'result': result}
        except Exception as e:
            error_detail = {
                'error': str(e),
                'traceback': traceback.format_exc()
            }
            raise HTTPException(
                status_code=500,
                detail=error_detail
            )

    @app.get('/health')
    async def health_check():
        return {'status': 'ok'}
''')


def generate_fastapi_app(handler_dir: Path, handler_config):
        """Генерирует файл FastAPI приложения"""
        try:
            app_code = FASTAPI_HANDLER_TEMPLATE.substitute(
                module=handler_config.interface_func_module,
                function=handler_config.interface_func_name)
            app_file = handler_dir / 'handler_app.py'
            app_file.write_text(app_code)

        except Exception as e:
            logger.error(
                f'‼️ Error generating FastAPI app '
                f'for {handler_config.handler_id}: {e}')
            logger.debug(f'{traceback.format_exc()}')
            raise
