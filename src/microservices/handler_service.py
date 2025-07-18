import traceback
from pathlib import Path
from string import Template

from loguru import logger
from pydantic import BaseModel
from typing_extensions import Any

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

class HandlerService:
    def __init__(self, handler_config: BaseModel):
        self._handler_config = handler_config

    def __dir__(self):
        """Add _handler_config fields to dir for IDE autocomplete"""
        return (list(super().__dir__())
                + list(self._handler_config.model_fields.keys()))

    def __getattr__(self, name: str) -> Any:
        """Get handler config attrs"""
        return getattr(self._handler_config, name)

    def generate_fastapi_app(self, handler_dir: Path):
            """Generate FastAPI app file"""
            try:
                app_code = FASTAPI_HANDLER_TEMPLATE.substitute(
                    module=self.interface_func_module,
                    function=self.interface_func_name)
                app_file = handler_dir / 'handler_app.py'
                app_file.write_text(app_code)

            except Exception as e:
                logger.error(
                    f'‼️ Error generating FastAPI app '
                    f'for {self.handler_id}: {e}')
                logger.debug(f'{traceback.format_exc()}')
                raise
