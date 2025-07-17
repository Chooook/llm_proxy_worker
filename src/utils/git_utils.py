import asyncio
import os
import subprocess
import traceback
from pathlib import Path

from loguru import logger

git_login = os.getenv('GIT_LOGIN')
git_pass = os.getenv('GIT_PASS')
GIT_CREDENTIALS = f'{git_login}:{git_pass}'


async def ensure_repo(handler_dir: Path, handler_config) -> bool:
    """Обеспечивает наличие актуальной версии репозитория"""
    try:
        if handler_dir.exists():
            return await update_repo(handler_dir, handler_config)
        else:
            return await clone_repo(handler_dir, handler_config)
    except Exception:
        logger.error(
            f'‼️ Repository operation failed '
            f'for {handler_config.handler_id}:\n'
            f'{traceback.format_exc()}'
        )
        # FIXME double traceback
        raise

async def clone_repo(target_dir: Path, handler_config) -> bool:
    """Клонирует Git репозиторий с ограничением глубины"""
    try:
        repo_url = augment_url_with_credentials(
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
                f'‼️ Git clone failed for {handler_config.handler_id}:\n'
                f'Command: {" ".join(command)}\n'
                f'Exit code: {process.returncode}\n'
                f'stdout: {stdout.decode()}\n'
                f'stderr: {stderr.decode()}'
            )
            return False
        return True
    except Exception:
        logger.error(
            f'‼️ Git clone exception for {handler_config.handler_id}:\n'
            f'{traceback.format_exc()}'
        )
        return False

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
                f'‼️ Git reset failed for {handler_config.handler_id}:\n'
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
                f'‼️ Git pull failed for {handler_config.handler_id}:\n'
                f'stdout: {pull_stdout.decode()}\n'
                f'stderr: {pull_stderr.decode()}'
            )
            return False

        return True
    except Exception:
        logger.error(
            f'‼️ Git update exception for {handler_config.handler_id}:\n'
            f'{traceback.format_exc()}'
        )
        return False
    finally:
        if current_dir:
            os.chdir(current_dir)

def augment_url_with_credentials(url: str) -> str:
    """Добавляет учетные данные в URL Git"""
    try:
        if not GIT_CREDENTIALS or '@' in url:
            return url

        if url.startswith('https://'):
            return f'https://{GIT_CREDENTIALS}@{url[8:]}'
        return url
    except Exception:
        logger.error(
            f'‼️ Error augmenting URL with credentials:\n'
            f'{traceback.format_exc()}'
        )
        return url
