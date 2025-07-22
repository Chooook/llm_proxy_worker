import asyncio
import json
import os
import shutil
import time
import traceback
from pathlib import Path
from string import Template

import httpx
from loguru import logger
from typing_extensions import Any, Optional

from schemas.handler import HandlerConfig
from schemas.task import Task
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
        self.host = '127.0.0.1'
        self.port: Optional[int] = None
        self._fastapi_process: Optional[asyncio.subprocess.Process] = None
        self.last_active_time: Optional[float] = None
        self._process_lock = asyncio.Lock()

    def __dir__(self):
        """Add _handler_config fields to dir for IDE autocomplete"""
        return (list(super().__dir__())
                + list(self._handler_config.model_fields.keys()))

    def __getattr__(self, name: str) -> Any:
        """Get handler config attrs"""
        return getattr(self._handler_config, name)

    @property
    def is_active(self):
        return bool(self._fastapi_process)

    async def process_task(self, task: Task, timeout: int = 420):
        if self._fastapi_process is None:
            async with self._process_lock:
                if self._fastapi_process is None:  # double check for race
                    if not await self.start(restart=True):
                        error_text = (f'‼️ Handler {self.handler_id} '
                                      f'restart failure!')
                        logger.error(error_text)
                        raise RuntimeError(error_text)

        self.last_active_time = time.time()
        process_endpoint = f'http://{self.host}:{self.port}/process'
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                process_endpoint, json=task.model_dump())
            if response.status_code != 200:
                error_text = (f'‼️ Handler {self.handler_id} error: '
                              f'{response.status_code} - {response.text}')
                logger.error(error_text)
                raise RuntimeError(error_text)

        result_str = response.json().get('result', '')
        try:
            result = json.loads(result_str)
        except json.JSONDecodeError:
            result = result_str
        return result

    async def prepare_executables(self):
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
            app_file_path = self._handler_dir / 'handler_app.py'
            try:
                app_code = FASTAPI_HANDLER_TEMPLATE.substitute(
                    module=self.interface_func_module,
                    function=self.interface_func_name)
                app_file_path.write_text(app_code)

            except Exception as e:
                logger.error(
                    f'‼️ Error generating FastAPI app '
                    f'for {self.handler_id}: {e}')
                logger.debug(f'{traceback.format_exc()}')
                raise

    async def start(self, port: int = 0, restart: bool = False):
        if self._fastapi_process:
            logger.info(f'ℹ️ Handler {self.handler_id} '
                        f'already started on {self.port}')
            return self._fastapi_process

        if port:
            self.port = port
        if not self.port:
            raise ValueError(f'Handler {self.handler_id} not started: '
                             f'port is not set!')

        self._fastapi_process = await asyncio.create_subprocess_exec(
            'uvicorn', 'handler_app:app',
            '--host', self.host,
            '--port', str(self.port),
            '--timeout-keep-alive', str(settings.HANDLER_INACTIVITY_TIMEOUT),
            cwd=str(self._handler_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        if not restart:
            if not await self.verify():
                await self.stop()  # "stop" set _fastapi_process to None
        await asyncio.sleep(2)  # wait for uvicorn to start

        return self._fastapi_process

    async def stop(self):
        try:
            if (self._fastapi_process
                    and self._fastapi_process.returncode is None):
                self._fastapi_process.terminate()
                try:
                    await asyncio.wait_for(
                        self._fastapi_process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    self._fastapi_process.kill()
                    await self._fastapi_process.wait()
                logger.info(f'ℹ️ Stopped handler {self.handler_id} '
                            f'on port {self.port}')
                self._fastapi_process = None
            return self.port
        except Exception as e:
            logger.error(
                f'‼️ Error stopping handler {self.handler_id}: {e}')
            logger.debug(f'{traceback.format_exc()}')
            self._fastapi_process = None
            return self.port

    async def verify(self) -> bool:
        try:
            if not await self._healthcheck():
                return False
            return await self._test_task_check()
        except Exception as e:
            logger.error(
                f'‼️ Handler verification failed '
                f'for {self.handler_id}: {e}')
            logger.debug(f'{traceback.format_exc()}')
            return False

    async def _healthcheck(self, retries: int = 5) -> bool:
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

    async def _test_task_check(self) -> bool:
        test_task = Task(
            handler_id=self.handler_id,
            prompt='test_task',
            task_id='test_task')
        try:
            await self.process_task(test_task)
            return True
        except Exception as e:
            logger.error(
                f'‼️ Test task verification failed '
                f'for {self.handler_id}: {e}')
            logger.debug(f'{traceback.format_exc()}')
            return False
