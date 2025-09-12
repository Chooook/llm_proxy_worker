import asyncio
import os
import shutil
from pathlib import Path

from loguru import logger


async def ensure_repo(repo: str, branch: str, target_dir: Path):
    git_token = os.getenv('GIT_TOKEN', '')
    # if not git_token:
    #     raise RuntimeError('GIT_TOKEN env var is not set')

    if target_dir.exists() and check_repo_dir_config(target_dir, repo):
        await update_repo(repo, branch, git_token, target_dir)
    else:
        shutil.rmtree(target_dir, ignore_errors=True)
        await clone_repo(repo, branch, git_token, target_dir)

def check_repo_dir_config(repo_dir: Path, repo: str):
    git_dir = repo_dir / '.git'
    if git_dir.exists():
        with open(git_dir / 'config') as f:
            for line in f:
                if line.strip().startswith('url ='):
                    config_repo = line.split('=')[1].strip()
                    if config_repo == repo:
                        return True
    return False

async def clone_repo(repo: str, branch: str, git_token: str, target_dir: Path):
    logger.info(f'⬇️ Cloning {repo}, branch: {branch} to {target_dir}...')
    command = [
        'git', 'clone',
        '--depth', '1',
        '--branch', branch,
        repo, str(target_dir)
    ]
    if git_token:
        command.extend(
            ['-c', f'http.extraHeader=Authorization: Bearer {git_token}']
        )
    process = await asyncio.create_subprocess_exec(
        *command, stderr=asyncio.subprocess.PIPE)
    _, stderr = await process.communicate()

    # remove token from config
    unset_token_command = ['git', 'config', '--unset', 'http.extraHeader']
    await asyncio.create_subprocess_exec(*unset_token_command, cwd=target_dir)

    if process.returncode == 0:
        logger.success(f'✅️ Git clone success from {repo}')
    else:
        logger.error(f'‼️ Git clone failed from {repo}')
        error = stderr.decode()
        raise RuntimeError(f'{error}')


async def update_repo(repo: str, branch: str, git_token:str, target_dir: Path):
    logger.info(f'⬇️ Updating {repo}, branch: {branch} in {target_dir}...')
    init_commands = [
        ['git', 'reset', '--hard', 'HEAD'],
        ['git', 'checkout', branch]
    ]

    for cmd in init_commands:
        process = await asyncio.create_subprocess_exec(
            *cmd, cwd=target_dir, stderr=asyncio.subprocess.PIPE)
        _, stderr = await process.communicate()

        if process.returncode != 0:
            logger.error(f'‼️ Git update cmd failed: {cmd}')
            error = stderr.decode()
            raise RuntimeError(f'{error}')

    pull_command = ['git', 'pull', 'origin', branch]
    if git_token:
        pull_command.extend(
            ['-c', f'http.extraHeader=Authorization: Bearer {git_token}']
        )
    update_process = await asyncio.create_subprocess_exec(
        *pull_command, cwd=target_dir, stderr=asyncio.subprocess.PIPE)
    _, stderr = await update_process.communicate()

    # remove token from config
    unset_token_command = ['git', 'config', '--unset', 'http.extraHeader']
    await asyncio.create_subprocess_exec(*unset_token_command, cwd=target_dir)

    if update_process.returncode == 0:
        logger.success(f'✅️ Git repo update success from {repo}')
    else:
        logger.error(f'‼️ Git repo update failed from {repo}, '
                     f'command: {pull_command}')
        error = stderr.decode()
        raise RuntimeError(f'{error}')
