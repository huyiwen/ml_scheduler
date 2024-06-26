import inspect
from typing import Any, Tuple

from .exp import Exp
from .runner.csv import CSVRunner


class ExpFunc:

    def __init__(self, exp_func) -> None:
        self.exp_func = exp_func
        self.run_csv = CSVRunner.set(self).run

    async def __call__(self, exp: Exp, **kwargs) -> Tuple[Exp, Any]:
        assert isinstance(exp, Exp)

        # filter out kwargs that are not needed
        need_kwargs = inspect.signature(self.exp_func).parameters.keys()
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in need_kwargs}
        ba = inspect.signature(self.exp_func).bind(exp, **filtered_kwargs)
        ba.apply_defaults()

        results = await self.exp_func(**ba.arguments)
        await exp.cleanup()
        return (exp, results)


def exp_func(func):
    return ExpFunc(func)
