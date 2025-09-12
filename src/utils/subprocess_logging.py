import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, List, Optional

from loguru import logger


@dataclass
class ManagedProcess:
    """Container for managed subprocess with logging"""
    process: asyncio.subprocess.Process
    name: str
    logging_tasks: List[asyncio.Task]
    monitor_task: Optional[asyncio.Task] = None

    async def wait(self) -> int:
        return await self.process.wait()

    async def stop(self, timeout: float = 5.0) -> bool:
        try:
            if self.monitor_task:
                self.monitor_task.cancel()

            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(),
                                           timeout=timeout)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await self.process.wait()

            for task in self.logging_tasks:
                task.cancel()

            # wait for all tasks to complete
            await asyncio.gather(*self.logging_tasks, return_exceptions=True)

            if self.monitor_task:
                await asyncio.wait_for(self.monitor_task, timeout=1.0)

            return True

        except Exception as e:
            logger.error(f'‼️ Error stopping process {self.name}: {e}')
            return False

    @property
    def returncode(self) -> Optional[int]:
        return self.process.returncode

    @property
    def is_running(self) -> bool:
        return self.process.returncode is None


async def run_managed_process(
        command: list,
        process_name: str,
        cwd: Optional[Path] = None,
        env: Optional[dict] = None,
        timeout: Optional[float] = None,
        success_callback: Optional[Callable[[], Any]] = None,
        error_callback: Optional[Callable[[Exception], Any]] = None
) -> ManagedProcess:

    logger.debug(f'ℹ️ Starting managed process: {process_name}')

    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def handle_stream(stream, is_stderr: bool = False):
        while True:
            try:
                line = await stream.readline()
                if not line:
                    break

                output = line.decode().strip()
                if not output:
                    continue

                logger.debug(f'{process_name}: {output}')

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f'⚠️ {process_name}: Error reading stream: {e}')
                break

    stdout_task = asyncio.create_task(handle_stream(process.stdout))
    stderr_task = asyncio.create_task(handle_stream(process.stderr, True))
    logging_tasks = [stdout_task, stderr_task]

    async def monitor_process():
        try:
            return_code = await asyncio.wait_for(
                process.wait(), timeout=timeout)

            # stop logging if process is not running
            for task in logging_tasks:
                task.cancel()
            await asyncio.gather(*logging_tasks, return_exceptions=True)

            if return_code == 0:
                logger.success(f'✅️ {process_name}: Completed successfully')
                if success_callback:
                    success_callback()
            else:
                logger.error(
                    f'‼️ {process_name}: Failed with code {return_code}')
                if error_callback:
                    error_callback(RuntimeError(
                        f'Process failed with code {return_code}'))

        except asyncio.TimeoutError:
            logger.error(
                f'‼️ {process_name}: Process timeout after {timeout}s')
            if error_callback:
                error_callback(
                    TimeoutError(f'Process {process_name} timed out'))
        except Exception as e:
            logger.error(f'‼️ {process_name}: Unexpected error: {e}')
            if error_callback:
                error_callback(e)

    monitor_task = asyncio.create_task(monitor_process())

    return ManagedProcess(
        process=process,
        name=process_name,
        logging_tasks=logging_tasks,
        monitor_task=monitor_task
    )
