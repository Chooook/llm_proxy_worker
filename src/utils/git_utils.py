import asyncio
import os
import subprocess
from pathlib import Path

from loguru import logger

from schemas.handler import HandlerConfig

# TODO: add credentials request to init script
GIT_LOGIN = os.getenv('GIT_LOGIN', '')
GIT_PASS = os.getenv('GIT_PASS', '')


async def ensure_repo(
        handler_dir: Path, handler_config: HandlerConfig):
    try:
        if handler_dir.exists():
            await update_repo(handler_dir, handler_config)
        else:
            await clone_repo(handler_dir, handler_config)
    except Exception:
        logger.error(
            f'‼️ Repository operation failed '
            f'for {handler_config.handler_id}'
        )
        raise

async def clone_repo(target_dir: Path, handler_config: HandlerConfig):
    command = [
        'git', 'clone',
        '--depth', '1',
        '--branch', handler_config.git_branch,
        handler_config.git_repo, str(target_dir)
    ]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await asyncio.sleep(1)  # wait for login query
    await process.communicate(input=f'{GIT_LOGIN}\n'.encode())
    await process.communicate(input=f'{GIT_PASS}\n'.encode())

    if process.returncode == 0:
        logger.success(f'✅️ Git clone success for {handler_config.handler_id}')
    else:
        logger.error(f'‼️ Git clone failed for {handler_config.handler_id}')
        error = await process.stderr.read()
        raise RuntimeError(f'{error.decode()}')


async def update_repo(repo_dir: Path, handler_config: HandlerConfig):
    reset_command = [
        'cd', str(repo_dir), '&&',
        'git', 'reset', '--hard', 'HEAD'
    ]
    reset_process = await asyncio.create_subprocess_exec(
        *reset_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if reset_process.returncode == 0:
        logger.success(f'✅️ Git reset success for {handler_config.handler_id}')
    else:
        logger.error(f'‼️ Git reset failed for {handler_config.handler_id}')
        error = await reset_process.stderr.read()
        raise RuntimeError(f'{error.decode()}')

    pull_command = [
        'cd', str(repo_dir), '&&',
        'git', 'pull', 'origin', handler_config.git_branch
    ]
    pull_process = await asyncio.create_subprocess_exec(
        *pull_command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await asyncio.sleep(1)  # wait for login query
    await pull_process.communicate(input=f'{GIT_LOGIN}\n'.encode())
    await pull_process.communicate(input=f'{GIT_PASS}\n'.encode())

    if pull_process.returncode == 0:
        logger.success(f'✅️ Git pull success for {handler_config.handler_id}')
    else:
        logger.error(f'‼️ Git pull failed for {handler_config.handler_id}')
        error = await pull_process.stderr.read()
        raise RuntimeError(f'{error.decode()}')
