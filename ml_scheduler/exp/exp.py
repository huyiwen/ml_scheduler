import asyncio
import subprocess
import threading
from logging import getLogger
from typing import Any, Dict, List, Set

from ..pools.base import BaseAllocator, BaseResources
from .runner import BaseRunner

logger = getLogger(__name__)


class Exp:

    def __init__(self, runner: BaseRunner, uuid: str):
        self.runner = runner
        self.uuid = uuid
        self.resources: Set[BaseResources] = set()

    async def get(self, alloc: BaseAllocator, *args, **kwargs):
        resource = await alloc(*args, **kwargs)
        self.resources.add(resource)
        return resource

    async def cleanup(self):
        for resource in self.resources:
            logger.debug(f"Cleaning up {resource}")
            await resource.cleanup()
        self.resources.clear()

    async def run(self, args: List[str], env: Dict[str, str], **kwargs) -> str:

        returns = None

        def run_in_thread(**popen_kwargs):
            proc = subprocess.Popen(**popen_kwargs)
            proc.wait()
            nonlocal returns
            returns = proc.returncode, proc.stdout.read().decode()
            return

        popen_kwargs = {
            "args": args,
            "env": env,
            "stdout": subprocess.PIPE,
        }

        thread = threading.Thread(target=run_in_thread,
                                  kwargs={
                                      **popen_kwargs,
                                      **kwargs
                                  })
        thread.start()

        while thread.is_alive():
            await asyncio.sleep(1)

        returncode, stdout = returns
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, stdout)

        return stdout

    async def report(self, metrics: Dict[str, Any], **kwargs):
        metrics.update(kwargs)
        await self.runner._report(self.uuid, metrics)
