import os
import shutil
import traceback
from pathlib import Path
from string import Template

from loguru import logger
from typing_extensions import Any, Optional

from schemas.handler import HandlerConfig
from utils import git_utils

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
    def __init__(self, handler_config: HandlerConfig):
        self._handler_config = handler_config
        self._handler_dir = (Path(os.getcwd())
                             / 'handlers'
                             / handler_config.handler_id.replace(':', '_'))
        # `:` is not allowed symbol for dir names in Windows
        self._app_file_path = self._handler_dir / 'handler_app.py'
        self.port: Optional[int] = None

    def __dir__(self):
        """Add _handler_config fields to dir for IDE autocomplete"""
        return (list(super().__dir__())
                + list(self._handler_config.model_fields.keys()))

    def __getattr__(self, name: str) -> Any:
        """Get handler config attrs"""
        return getattr(self._handler_config, name)

    async def prepare_handler_executables(self):
        """Clone or copy handler executables to handler dir"""
        if self.git_repo:
            if not await git_utils.ensure_repo(self):
                return False
        else:
            shutil.rmtree(self._handler_dir, ignore_errors=True)
            self._handler_dir.mkdir(exist_ok=True, parents=True)
            shutil.copytree(self.source_dir_name, self._handler_dir)
        return True

    def generate_fastapi_app(self):
            """Generate FastAPI app file"""
            try:
                app_code = FASTAPI_HANDLER_TEMPLATE.substitute(
                    module=self.interface_func_module,
                    function=self.interface_func_name)
                app_file = self._app_file_path
                app_file.write_text(app_code)

            except Exception as e:
                logger.error(
                    f'‼️ Error generating FastAPI app '
                    f'for {self.handler_id}: {e}')
                logger.debug(f'{traceback.format_exc()}')
                raise
