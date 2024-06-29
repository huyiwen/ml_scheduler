import inspect
from logging import getLogger
from traceback import format_exc
from typing import Any, Tuple

from .exp import Exp
from .runner.csv import CSVRunner
from .runner.sqlite import SQLiteRunner

logger = getLogger(__name__)


class ExpFunc:

    def __init__(self, exp_func) -> None:
        self.exp_func = exp_func

        csv_runner = CSVRunner.set(self)
        self.run_csv = csv_runner.run
        self.arun_csv = csv_runner.arun

        sqlite_runner = SQLiteRunner.set(self)
        self.run_sqlite = sqlite_runner.run
        self.arun_sqlite = sqlite_runner.arun

    async def __call__(self, exp: Exp, **kwargs) -> Tuple[Exp, Any]:
        assert isinstance(exp, Exp)

        # filter out kwargs that are not needed
        need_kwargs = inspect.signature(self.exp_func).parameters.keys()
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in need_kwargs}
        ba = inspect.signature(self.exp_func).bind(exp, **filtered_kwargs)
        ba.apply_defaults()

        # change nan to None
        exp_func_kwargs = dict(ba.arguments)
        for key, value in exp_func_kwargs.items():
            if value == float('nan'):
                exp_func_kwargs[key] = None

        try:
            results = await self.exp_func(**exp_func_kwargs)
        except Exception as e:
            logger.warning(format_exc())
            logger.error(f"Error in {self.exp_func.__name__}: {e}")
            results = ""

        await exp.cleanup()
        return (exp, results)


def exp_func(func):
    return ExpFunc(func)
