import asyncio
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict

from typing_extensions import Self

if TYPE_CHECKING:
    from ..func import ExpFunc

logger = getLogger(__name__)


class BaseRunner:

    exp_func: "ExpFunc"

    @classmethod
    def set(cls, exp_func: "ExpFunc") -> "Self":

        class RunnerWithExpFunc(cls):

            def __new__(cls) -> Self:
                self = super().__new__(cls)
                self.exp_func = exp_func
                return self

        return RunnerWithExpFunc()

    def create_task(self, uuid: str, **kwargs):
        from ..exp import Exp
        logger.info(f"Create task: {uuid}")
        return asyncio.create_task(self.exp_func(Exp(self, uuid), **kwargs),
                                   name=uuid)

    async def _write_cell(self, uuid: str, metric: str, value: Any):
        raise NotImplementedError("_write_cell method is not implemented")

    async def _report(self, uuid: str, metrics: Dict[str, Any]):
        logger.info(f"Reporting {uuid} {metrics}")
        for metric, value in metrics.items():
            await self._write_cell(uuid, metric, value)

    def run(self, *args, **kwargs):
        raise NotImplementedError("run method is not implemented")

    async def arun(self, *args, **kwargs):
        raise NotImplementedError("run method is not implemented")
