import asyncio
import os
import shutil
from logging import getLogger
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas

from .base import BaseRunner

logger = getLogger(__name__)


class CSVRunner(BaseRunner):

    def run(
        self,
        csv_path: str,
        continue_cols: List[str],
        force_rerun: bool = False,
        read_csv_kwargs: Optional[dict] = None,
        uuid_column: str = ":uuid:",
        retval_column: Optional[str] = ":retval:",
    ):
        """Run experiments from a csv file

        Args:
            csv_path (`str`): The path to the csv file.
            continue_cols (`List[str]`): Run experiments where the columns are null.
            force_rerun (`bool`, optional): Force rerun all experiments. Ignore `continue_cols`. Defaults to False.
            read_csv_kwargs (`Optional[dict]`, optional): Additional kwargs passed to `pandas.read_csv`. Defaults to None.
            uuid_column (`str`, optional): The column name for the uuid. Defaults to `":uuid:"`.
            retval_column (`Optional[str]`, optional): The column name for the return value. None for not saving the return value. Defaults to `":retval:"`.
        """
        kwargs = {
            "csv_path": csv_path,
            "continue_cols": continue_cols,
            "force_rerun": force_rerun,
            "read_csv_kwargs": read_csv_kwargs or {},
            "uuid_column": uuid_column,
            "retval_column": retval_column,
        }
        return asyncio.run(self.arun(**kwargs))

    def submit_from_csv(
        self,
        force_rerun: bool = False,
    ):
        if os.path.exists(self.csv_path + ".lock"):
            os.remove(self.csv_path + ".lock")
        df: pandas.DataFrame = pandas.read_csv(self.csv_path,
                                               **self.read_csv_kwargs)

        # set uuid
        if self.uuid_column not in df.columns:
            df[self.uuid_column] = [str(uuid4()) for _ in range(len(df))]
        else:
            rows = df[self.uuid_column].isnull()
            df.loc[rows, self.uuid_column] = [
                str(uuid4()) for _ in range(len(df[rows]))
            ]

        df = df.set_index(self.uuid_column)
        df.to_csv(self.csv_path, index=True)

        # force rerun
        if not force_rerun:
            rows = False
            for col in self.continue_cols:
                rows |= df[col].isnull()
            added = int(rows.sum())
            logger.info(f"Adding {added} tasks ({len(df) - added} skipped).")
        else:
            rows = slice(None)
            logger.info(f"Adding {len(df)} tasks.")

        tasks = [
            self.create_task(uuid, **row) for uuid, row in df[rows].iterrows()
        ]

        return tasks

    async def __write_cell(self, row, col, value):

        lock_file = self.csv_path + ".lock"
        timeout = 10
        counts = 0
        while os.path.exists(lock_file):
            await asyncio.sleep(0.1)
            counts += 1
            if counts == timeout:
                logger.error(f"Timeout waiting for lock file {lock_file}")
                return

        shutil.copy(self.csv_path, lock_file)
        df = pandas.read_csv(self.csv_path, index_col=self.uuid_column)
        df.loc[row, col] = value
        df.to_csv(self.csv_path, index=True)
        os.remove(lock_file)

    async def _report(self, uuid: str, metrics: Dict[str, Any]):
        logger.info(f"Reporting {uuid} {metrics}")
        for metric, value in metrics.items():
            await self.__write_cell(uuid, metric, value)

    async def arun(
        self,
        csv_path: str,
        continue_cols: List[str],
        force_rerun: bool = False,
        read_csv_kwargs: Optional[dict] = None,
        uuid_column: str = ":uuid:",
        retval_column: Optional[str] = ":retval:",
    ):
        """Async run experiments from a csv file"""

        self.csv_path = csv_path
        self.continue_cols = continue_cols
        self.read_csv_kwargs = read_csv_kwargs
        self.uuid_column = uuid_column

        tasks = self.submit_from_csv(force_rerun)

        # block until all tasks are done
        for task in asyncio.as_completed(tasks):
            exp, results = await task
            logger.info(f"Finished {exp.uuid}")
            if retval_column is not None:
                await self.__write_cell(exp.uuid, retval_column, results)
