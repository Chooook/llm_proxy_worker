import asyncio
import os
from pathlib import Path

from loguru import logger

# TODO: add credentials request to init script
GIT_LOGIN = os.getenv('GIT_LOGIN', '')
GIT_PASS = os.getenv('GIT_PASS', '')


async def ensure_repo(repo: str, branch: str, target_dir: Path):
    if target_dir.exists():
        await update_repo(repo, branch, target_dir)
    else:
        await clone_repo(repo, branch, target_dir)

async def clone_repo(repo: str, branch: str, target_dir: Path):
    logger.info(f'⬇️ Cloning {repo}, branch: {branch} to {target_dir}...')
    command = [
        'git', 'clone',
        '--depth', '1',
        '--branch', branch,
        repo, str(target_dir)
    ]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=None,
        stderr=asyncio.subprocess.PIPE,
    )
    await asyncio.sleep(1)  # wait for auth query
    _, stderr = await process.communicate(
        input=f'{GIT_LOGIN}\n{GIT_PASS}\n'.encode())

    if process.returncode == 0:
        logger.success(f'✅️ Git clone success from {repo}')
    else:
        logger.error(f'‼️ Git clone failed from {repo}')
        error = stderr.decode()
        raise RuntimeError(f'{error}')


async def update_repo(repo: str, branch: str, target_dir: Path):
    logger.info(f'⬇️ Updating {repo}, branch: {branch} in {target_dir}...')
    init_commands = [
        ['git', 'reset', '--hard', 'HEAD'],
        ['git', 'checkout', branch]
    ]

    for cmd in init_commands:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=target_dir,
            stdout=None,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f'‼️ Git update cmd failed: {cmd}')
            error = stderr.decode()
            raise RuntimeError(f'{error}')

    pull_command = ['git', 'pull', 'origin', branch]
    update_process = await asyncio.create_subprocess_exec(
        *pull_command,
        cwd=target_dir,
        stdin=asyncio.subprocess.PIPE,
        stdout=None,
        stderr=asyncio.subprocess.PIPE
    )
    await asyncio.sleep(1)  # wait for auth query
    _, stderr = await update_process.communicate(
        input=f'{GIT_LOGIN}\n{GIT_PASS}\n'.encode())

    if update_process.returncode == 0:
        logger.success(f'✅️ Git repo update success from {repo}')
    else:
        logger.error(f'‼️ Git repo update failed from {repo}, '
                     f'command: {pull_command}')
        error = stderr.decode()
        raise RuntimeError(f'{error}')
