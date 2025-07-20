import asyncio
import os
import shutil
import traceback
from pathlib import Path
from string import Template

import httpx
from loguru import logger
from typing_extensions import Any, Optional

from schemas.handler import HandlerConfig
from settings import settings
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
        self._handler_dir = (
                Path(os.getcwd())
                / 'handlers'
                / handler_config.handler_id.replace(':', '_')
        ).resolve()
        # `:` is not allowed symbol for dir names in Windows
        self._app_file_path = self._handler_dir / 'handler_app.py'
        self.host = '127.0.0.1'
        self.port: Optional[int] = None
        self._fastapi_process: Optional[asyncio.subprocess.Process] = None

    def __dir__(self):
        """Add _handler_config fields to dir for IDE autocomplete"""
        return (list(super().__dir__())
                + list(self._handler_config.model_fields.keys()))

    def __getattr__(self, name: str) -> Any:
        """Get handler config attrs"""
        return getattr(self._handler_config, name)

    async def prepare_handler_executables(self):
        """Clone or copy handler executables to handler dir"""
        if self.git_repo:  # FIXME: update git_utils
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

    async def start_handler(self):
        if not self.port:
            raise ValueError(f'Handler {self.handler_id} port is not set!')
        if self._fastapi_process:
            return self._fastapi_process
        self._fastapi_process = await asyncio.create_subprocess_exec(
            'uvicorn', 'handler_app:app',
            '--host', self.host,
            '--port', str(self.port),
            '--timeout-keep-alive', str(settings.HANDLER_INACTIVITY_TIMEOUT),
            cwd=str(self._handler_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        return self._fastapi_process

    async def healthcheck(self, retries: int = 5) -> bool:
        try:
            url = f'http://{self.host}:{self.port}/health'
            async with httpx.AsyncClient(timeout=3) as client:
                for _ in range(retries):
                    try:
                        response = await client.get(url)
                        if response.status_code == 200:
                            return True
                    except (httpx.ConnectError, httpx.ReadTimeout):
                        await asyncio.sleep(1)
            return False
        except Exception as e:
            logger.error(
                f'‼️ Health check failed for handler service '
                f'{self.handler_id}: {e}')
            logger.debug(f'{traceback.format_exc()}')
            return False
