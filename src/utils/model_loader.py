import asyncio
import hashlib
import json
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from loguru import logger

from schemas.local_model import LocalModel
from settings import settings


async def download_models(max_concurrent: int = 3) -> List[LocalModel]:

    base_dir = Path(settings.LOCAL_MODELS_PATH)
    hash_file = base_dir / 'model_hashes.json'
    models = settings.local_models
    token = settings.MIRROR_TOKEN
    base_dir.mkdir(parents=True, exist_ok=True)

    # read loaded models hashes from file
    hash_history = await _load_hash_history(hash_file)

    semaphore = asyncio.Semaphore(max_concurrent)

    async def download_with_semaphore(model):
        async with semaphore:
            return await _download_single_model(
                model, base_dir, hash_history, token
            )

    # start downloading tasks in parallel
    tasks = [download_with_semaphore(model) for model in models]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    updated_models = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f'‼️ Error downloading {models[i].name}: {result}')
            updated_models.append(models[i])
        else:
            updated_models.append(result)

    await _save_hash_history(hash_file, hash_history)
    settings.local_models = results

    return updated_models


async def _load_hash_history(hash_file: Path) -> Dict[str, str]:
    if hash_file.exists():
        try:
            async with aiofiles.open(hash_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f'⚠️ Incorrect hashes file {hash_file}: {e}')
            return {}
    return {}


async def _download_single_model(
        model: LocalModel,
        base_dir: Path,
        hash_history: Dict[str, str],
        token: Optional[str] = None,
) -> LocalModel:

    try:
        model_dir = base_dir / model.name

        # check if model already downloaded
        if (not settings.FORCE_MODEL_DOWNLOAD
                and model_dir.exists()
                and model_dir.is_dir()):
            current_hash = await _calculate_dir_hash(model_dir)

            if (model.name in hash_history
                    and hash_history[model.name] == current_hash):
                logger.info(
                    f'ℹ️ Model {model.name} already downloaded and up to date')
                return LocalModel(
                    name=model.name,
                    link=model.link,
                    path=str(model_dir.absolute())
                )

        logger.info(f'ℹ️ Starting download of {model.name}...')

        if _is_archive_url(model.link):
            archive_ext = '.zip' if model.link.endswith('.zip') else '.tar.gz'
            archive_path = model_dir / f'{model.name}{archive_ext}'
            success = await _download_file_with_wget(
                model.link, archive_path, token)
            if not success:
                raise Exception(f'Unable to download {model.name} archive')

            await _extract_archive(archive_path, model_dir)

        else:
            filename = model.link.split('/')[-1]
            file_path = model_dir / filename

            success = await _download_file_with_wget(
                model.link, file_path, token)
            if not success:
                raise Exception(f'Unable to download {model.name} model file')

        model_hash = await _calculate_dir_hash(model_dir)
        hash_history[model.name] = model_hash

        logger.info(f'ℹ️ Model {model.name} successfully '
                    f'downloaded to {model_dir}')

        return LocalModel(
            name=model.name,
            link=model.link,
            path=str(model_dir.absolute())
        )

    except Exception as e:
        logger.error(f'‼️ Error downloading {model.name}: {e}')
        return model


async def _calculate_dir_hash(directory: Path,
                              algorithm: str = 'sha256') -> str:
    hash_func = getattr(hashlib, algorithm)()

    for root, dirs, files in os.walk(directory):
        for file in sorted(files):
            file_path = Path(root) / file
            rel_path = file_path.relative_to(directory)
            hash_func.update(str(rel_path).encode())

            async with aiofiles.open(file_path, 'rb') as f:
                while chunk := await f.read(8192):
                    hash_func.update(chunk)

    return hash_func.hexdigest()


def _is_archive_url(url: str) -> bool:
    return any(
        url.endswith(ext) for ext in ['.zip', '.tar.gz', '.tgz', '.tar'])


async def _download_file_with_wget(url: str, destination: Path,
                                   token: Optional[str] = None) -> bool:
    try:
        cmd = ['wget', '--no-check-certificate', '-O', str(destination)]

        if token:
            if 'huggingface.co' in url:
                cmd.extend(['--header', f'Authorization: Bearer {token}'])
            else:
                url = url.replace('http://', f'http://{token}@')
                url = url.replace('https://', f'https://{token}@')

        cmd.append(url)

        logger.info(f'ℹ️ Starting wget download for model {destination.name}')

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f'ℹ️ Model {destination.name} downloaded '
                        f'to {destination}')
            return True
        else:
            logger.error(f'‼️ Model {destination.name} '
                         f'download failed: {stderr.decode()}')
            return False

    except Exception as e:
        logger.error(f'‼️ Model {destination.name} download failed: {e}')
        return False


async def _extract_archive(archive_path: Path, extract_dir: Path):

    def sync_extract():
        extract_dir.mkdir(parents=True, exist_ok=True)

        if archive_path.suffix == '.zip':
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        elif archive_path.suffix in ['.tar.gz', '.tgz']:
            with tarfile.open(archive_path, 'r:gz') as tar_ref:
                tar_ref.extractall(extract_dir)
        else:
            raise ValueError(
                f'‼️ Unsupported archive format: {archive_path.suffix}')

    await asyncio.get_event_loop().run_in_executor(None, sync_extract)
    archive_path.unlink()


async def _save_hash_history(hash_file: Path, history: Dict[str, str]):
    hash_file.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(hash_file, 'w') as f:
        await f.write(json.dumps(history, indent=2))
