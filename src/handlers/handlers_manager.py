import asyncio
import os
import subprocess
import time
import traceback
from pathlib import Path
from typing import Dict, Optional

import httpx
from loguru import logger

from schemas.task import Task
from settings import settings


class HandlerManager:
    def __init__(self):
        self.active_handlers: Dict[str, dict] = {}
        self.verified_handlers: Dict[str, bool] = {}
        self.port_pool = set(range(*settings.HANDLER_PORT_RANGE))
        self.base_dir = Path(__file__).parent
        self.base_dir.mkdir(exist_ok=True, parents=True)
        self._lock = asyncio.Lock()
        git_login = os.getenv('GIT_LOGIN')
        git_pass = os.getenv('GIT_PASS')
        self.git_credentials = f'{git_login}:{git_pass}'
        self.test_task = Task(
            handler_id='',
            prompt='Тестовый запрос',
            task_id='test_task')

    async def verify_handler(self, handler_id: str) -> bool:
        """
        Проверяет, может ли обработчик быть запущен и работает ли корректно
        """
        if handler_id in self.verified_handlers:
            return self.verified_handlers[handler_id]

        try:
            port = await self.start_handler(handler_id)
            if port:
                await self.stop_handler(handler_id)
                self.verified_handlers[handler_id] = True
                return True
        except Exception as e:
            logger.error(f'Handler verification failed for {handler_id}: {e}')

        self.verified_handlers[handler_id] = False
        return False

    async def get_handler_url(self, handler_id: str) -> Optional[str]:
        """Возвращает URL обработчика, запуская его при необходимости"""
        async with self._lock:
            if handler_id in self.active_handlers:
                self.active_handlers[handler_id]['last_activity'] = time.time()
                port = self.active_handlers[handler_id]['port']
                return f'http://127.0.0.1:{port}/process'

            port = await self.start_handler(handler_id)
            if port:
                return f'http://127.0.0.1:{port}/process'
            return None

    async def start_handler(self, handler_id: str) -> Optional[int]:
        """Запускает FastAPI приложение для обработчика и возвращает порт"""
        try:
            handler_config = next(
                (h for h in settings.HANDLERS if h.handler_id == handler_id),
                None
            )
            if not handler_config:
                logger.error(
                    f'Handler {handler_id} not found in configuration')
                return None

            # FIXME: symbol : not allowed in windows dir names
            handler_dir = self.base_dir / handler_id
            if handler_config.git_repo:
                if not await self.ensure_repo(handler_dir, handler_config):
                    return None
            else:
                handler_dir.mkdir(exist_ok=True, parents=True)

            # Генерация FastAPI приложения
            self.generate_fastapi_app(handler_dir, handler_config)

            # Выбор порта
            if not self.port_pool:
                logger.error('No available ports for handler')
                return None
            port = self.port_pool.pop()

            # Запуск приложения
            process = await asyncio.create_subprocess_exec(
                'uvicorn', 'handler_app:app', '--host', '127.0.0.1', '--port',
                str(port),
                '--timeout-keep-alive',
                str(settings.HANDLER_INACTIVITY_TIMEOUT),
                cwd=str(handler_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # Проверка работоспособности
            if not await self.verify_handler_operation(port, handler_config):
                logger.error(f'Handler {handler_id} failed operational check')
                process.terminate()
                await process.wait()
                self.port_pool.add(port)
                return None

            # Регистрация успешного обработчика
            self.active_handlers[handler_id] = {
                'port': port,
                'process': process,
                'last_activity': time.time(),
                'dir': handler_dir,
                'config': handler_config
            }

            logger.info(f'Started handler {handler_id} on port {port}')
            return port

        except Exception:
            logger.error(
                f'Error starting handler {handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            # TODO traceback to logger.debug
            if 'port' in locals():
                self.port_pool.add(port)
            raise

    async def ensure_repo(self, handler_dir: Path, handler_config) -> bool:
        """Обеспечивает наличие актуальной версии репозитория"""
        try:
            if handler_dir.exists():
                return await self.update_repo(handler_dir, handler_config)
            else:
                return await self.clone_repo(handler_dir, handler_config)
        except Exception:
            logger.error(
                f'Repository operation failed '
                f'for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            # FIXME double traceback
            raise

    async def clone_repo(self, target_dir: Path, handler_config) -> bool:
        """Клонирует Git репозиторий с ограничением глубины"""
        try:
            repo_url = self.augment_url_with_credentials(
                handler_config.git_repo)
            command = [
                'git', 'clone',
                '--depth', '1',
                '--branch', handler_config.git_branch,
                repo_url, str(target_dir)
            ]

            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logger.error(
                    f'Git clone failed for {handler_config.handler_id}:\n'
                    f'Command: {" ".join(command)}\n'
                    f'Exit code: {process.returncode}\n'
                    f'stdout: {stdout.decode()}\n'
                    f'stderr: {stderr.decode()}'
                )
                return False
            return True
        except Exception:
            logger.error(
                f'Git clone exception for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            return False

    @staticmethod
    async def update_repo(repo_dir: Path, handler_config) -> bool:
        """Обновляет существующий репозиторий"""
        try:
            current_dir = os.getcwd()
            os.chdir(repo_dir)

            # Сброс изменений
            reset_process = await asyncio.create_subprocess_exec(
                'git', 'reset', '--hard', 'HEAD',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            reset_stdout, reset_stderr = await reset_process.communicate()

            if reset_process.returncode != 0:
                logger.error(
                    f'Git reset failed for {handler_config.handler_id}:\n'
                    f'stdout: {reset_stdout.decode()}\n'
                    f'stderr: {reset_stderr.decode()}'
                )
                return False

            # Обновление репозитория
            pull_process = await asyncio.create_subprocess_exec(
                'git', 'pull', 'origin', handler_config.git_branch,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            pull_stdout, pull_stderr = await pull_process.communicate()

            if pull_process.returncode != 0:
                logger.error(
                    f'Git pull failed for {handler_config.handler_id}:\n'
                    f'stdout: {pull_stdout.decode()}\n'
                    f'stderr: {pull_stderr.decode()}'
                )
                return False

            return True
        except Exception:
            logger.error(
                f'Git update exception for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            return False
        finally:
            if current_dir:
                os.chdir(current_dir)

    def augment_url_with_credentials(self, url: str) -> str:
        """Добавляет учетные данные в URL Git"""
        try:
            if not self.git_credentials or '@' in url:
                return url

            if url.startswith('https://'):
                return f'https://{self.git_credentials}@{url[8:]}'
            return url
        except Exception:
            logger.error(
                f'Error augmenting URL with credentials:\n'
                f'{traceback.format_exc()}'
            )
            return url

    @staticmethod
    def generate_fastapi_app(handler_dir: Path, handler_config):
        """Генерирует файл FastAPI приложения с обработкой ошибок"""
        try:
            app_code = f'''
import traceback
from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.post('/process')
async def process_request(data: dict):
    try:
        from {handler_config.interface_func_module} import {handler_config.interface_func_name}
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
                f'Error generating FastAPI app '
                f'for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            raise

    async def verify_handler_operation(self, port: int,
                                       handler_config) -> bool:
        """Проводит расширенную проверку работоспособности обработчика"""
        try:
            # 1. Проверка базового healthcheck
            if not await self.check_handler_health(port):
                return False

            # 2. Проверка выполнения тестовой задачи
            return await self.verify_with_test_task(port, handler_config)
        except Exception:
            logger.error(
                f'Handler verification failed '
                f'for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            return False

    @staticmethod
    async def check_handler_health(port: int, retries: int = 5) -> bool:
        """Проверяет доступность обработчика через healthcheck"""
        try:
            url = f'http://localhost:{port}/health'
            async with httpx.AsyncClient(timeout=5) as client:
                for _ in range(retries):
                    try:
                        response = await client.get(url)
                        if response.status_code == 200:
                            return True
                    except (httpx.ConnectError, httpx.ReadTimeout):
                        await asyncio.sleep(1)
            return False
        except Exception:
            logger.error(
                f'Health check failed for port {port}:\n'
                f'{traceback.format_exc()}'
            )
            return False

    async def verify_with_test_task(self, port: int, handler_config) -> bool:
        """Проверяет обработчик с помощью тестовой задачи"""
        try:
            url = f'http://0.0.0.0:{port}/process'

            # Выбор тестовой задачи
            test_task = self.test_task
            handler_id = handler_config.handler_id
            test_task.handler_id = handler_id

            async with httpx.AsyncClient(timeout=30) as client:
                # Отправка тестовой задачи
                response = await client.post(url, json=test_task.model_dump())

                if response.status_code != 200:
                    # Для 500 ошибок выводим детали из FastAPI
                    if response.status_code == 500:
                        error_detail = response.json().get('detail', {})
                        logger.error(
                            f'Test task failed '
                            f'for {handler_config.handler_id}:\n'
                            f'Error: {error_detail.get("error", "")}\n'
                            f'Traceback:\n{error_detail.get("traceback", "")}'
                        )
                    else:
                        logger.error(
                            f'Test task failed '
                            f'for {handler_config.handler_id}: '
                            f'{response.status_code} - {response.text}'
                        )
                    return False

                result = response.json()
                if 'result' not in result:
                    logger.error(
                        f'Invalid response format '
                        f'from {handler_config.handler_id}: '
                        f'missing "result" field'
                    )
                    return False

                return True
        except Exception:
            logger.error(
                f'Test task verification failed '
                f'for {handler_config.handler_id}:\n'
                f'{traceback.format_exc()}'
            )
            return False

    async def stop_inactive_handlers(self):
        """Останавливает неактивные обработчики"""
        try:
            current_time = time.time()
            to_stop = []

            for handler_id, info in self.active_handlers.items():
                if current_time - info[
                    'last_activity'] > settings.HANDLER_INACTIVITY_TIMEOUT:
                    to_stop.append(handler_id)

            for handler_id in to_stop:
                await self.stop_handler(handler_id)
        except Exception:
            logger.error(
                f'Error stopping inactive handlers:\n'
                f'{traceback.format_exc()}'
            )

    async def stop_handler(self, handler_id: str):
        """Останавливает обработчик и освобождает порт"""
        try:
            if handler_id not in self.active_handlers:
                return

            info = self.active_handlers.pop(handler_id)
            process = info['process']

            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()

            self.port_pool.add(info['port'])
            logger.info(f'Stopped handler {handler_id} '
                        f'on port {info["port"]}')
        except Exception:
            logger.error(
                f'Error stopping handler {handler_id}:\n'
                f'{traceback.format_exc()}'
            )

    async def cleanup(self):
        """Останавливает все обработчики при завершении"""
        try:
            for handler_id in list(self.active_handlers.keys()):
                await self.stop_handler(handler_id)
        except Exception:
            logger.error(
                f'Error during cleanup:\n'
                f'{traceback.format_exc()}'
            )

    async def monitor_inactive_handlers(self):
        """Фоновая задача для мониторинга неактивных обработчиков"""
        while True:
            try:
                await asyncio.sleep(60)
                await self.stop_inactive_handlers()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.error(
                    f'Error in inactive handlers monitor:\n'
                    f'{traceback.format_exc()}'
                )
