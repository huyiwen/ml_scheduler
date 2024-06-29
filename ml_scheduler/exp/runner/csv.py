import asyncio
import os
import shutil
from logging import getLogger
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas
import readchar

from ...threads import to_thread
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
        extra_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Run experiments from a csv file

        Args:
            csv_path (`str`): The path to the csv file.
            continue_cols (`List[str]`): Run experiments where the columns are null.
            force_rerun (`bool`, optional): Force rerun all experiments. Ignore `continue_cols`. Defaults to False.
            read_csv_kwargs (`Optional[dict]`, optional): Additional kwargs passed to `pandas.read_csv`. Defaults to None.
            uuid_column (`str`, optional): The column name for the uuid. Defaults to `":uuid:"`.
            retval_column (`Optional[str]`, optional): The column name for the return value. None for not saving the return value. Defaults to `":retval:"`.
            extra_kwargs (`Optional[Dict[str, Any]]`, optional): Extra kwargs passed to exp_func.
        """
        kwargs = {
            "csv_path": csv_path,
            "continue_cols": continue_cols,
            "force_rerun": force_rerun,
            "read_csv_kwargs": read_csv_kwargs,
            "uuid_column": uuid_column,
            "retval_column": retval_column,
            "extra_kwargs": extra_kwargs,
        }
        return asyncio.run(self.arun(**kwargs))

    def submit_from(
        self,
        force_rerun: bool = False,
    ):
        if os.path.exists("." + self.csv_path + ".lock"):
            logger.warning(
                f'Lock file ".{self.csv_path}.lock" already exists!\n"(C)ontinue anyway, (Q)uit:"'
            )
            op = None
            while op not in ["c", "q"]:
                op = readchar.readchar().lower()
            if op == "c":
                return
            os.remove("." + self.csv_path + ".lock")
        df: pandas.DataFrame = pandas.read_csv(
            self.csv_path,
            **self.read_csv_kwargs,
        )

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
                if col in df.columns:
                    rows |= df[col].isnull()
                else:
                    # needs to fill in the empty column
                    rows = False
                    break

            if isinstance(rows, bool):
                rows = slice(None)
                logger.info(f"Adding {len(df)} tasks.")
            else:
                added = int(rows.sum())
                logger.info(
                    f"Adding {added} tasks ({len(df) - added} skipped).")
        else:
            rows = slice(None)
            logger.info(f"Adding {len(df)} tasks.")

        tasks = [
            self.create_task(uuid, **row, **self.extra_kwargs)
            for uuid, row in df[rows].iterrows()
        ]

        return tasks

    async def _write_cell(self, row, col, value):

        lock_file = "." + self.csv_path + ".lock"
        timeout = 100
        counts = 0
        while os.path.exists(lock_file):
            await asyncio.sleep(0.01)
            counts += 1
            if counts == timeout:
                logger.error(f"Timeout waiting for lock file {lock_file}")
                return

        def _write_atomic(lock_file, csv_path, row, col, value):
            shutil.copy(csv_path, lock_file)
            try:
                df = pandas.read_csv(
                    csv_path,
                    index_col=self.uuid_column,
                    **self.read_csv_kwargs,
                )
                df.loc[row, col] = value
                df.to_csv(csv_path, index=True)
            except Exception as e:
                shutil.copy(lock_file, csv_path)
                logger.warning(f"Error writing to csv: {e}")
            finally:
                os.remove(lock_file)

        await to_thread(_write_atomic, lock_file, self.csv_path, row, col,
                        value)

    async def arun(
        self,
        csv_path: str,
        continue_cols: List[str],
        force_rerun: bool = False,
        read_csv_kwargs: Optional[dict] = None,
        uuid_column: str = ":uuid:",
        retval_column: Optional[str] = ":retval:",
        extra_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Async run experiments from a csv file"""

        self.csv_path = csv_path
        self.continue_cols = continue_cols
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.uuid_column = uuid_column
        self.extra_kwargs = extra_kwargs or {}

        tasks = self.submit_from(force_rerun)

        # block until all tasks are done
        for task in asyncio.as_completed(tasks):
            exp, results = await task
            logger.info(f"Finished {exp.uuid}")
            if retval_column is not None:
                await self._write_cell(exp.uuid, retval_column, results)
