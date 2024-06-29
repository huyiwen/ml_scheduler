import asyncio
import sqlite3
from logging import getLogger
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas

from ...threads import to_thread
from .base import BaseRunner

logger = getLogger(__name__)


class SQLiteRunner(BaseRunner):

    def run(
        self,
        sqlite_path: str,
        table_name: str,
        continue_cols: List[str],
        force_rerun: bool = False,
        uuid_column: str = ":uuid:",
        retval_column: Optional[str] = ":retval:",
        extra_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Run experiments from a csv file

        Args:
            sqlite_path (`str`): The path to the sqlite file.
            continue_cols (`List[str]`): Run experiments where the columns are null.
            force_rerun (`bool`, optional): Force rerun all experiments. Ignore `continue_cols`. Defaults to False.
            uuid_column (`str`, optional): The column name for the uuid. Defaults to `":uuid:"`.
            retval_column (`Optional[str]`, optional): The column name for the return value. None for not saving the return value. Defaults to `":retval:"`.
            extra_kwargs (`Optional[Dict[str, Any]]`, optional): Extra kwargs passed to exp_func.
        """
        kwargs = {
            "sqlite_path": sqlite_path,
            "table_name": table_name,
            "continue_cols": continue_cols,
            "force_rerun": force_rerun,
            "uuid_column": uuid_column,
            "retval_column": retval_column,
            "extra_kwargs": extra_kwargs,
        }
        return asyncio.run(self.arun(**kwargs))

    def submit_from(
        self,
        dbcon,
        force_rerun: bool = False,
    ):

        query = dbcon.execute(f"SELECT * FROM {self.table_name}")
        cols = [column[0] for column in query.description]
        df: pandas.DataFrame = pandas.DataFrame.from_records(
            data=query.fetchall(), columns=cols)

        # set uuid
        if self.uuid_column not in df.columns:
            df[self.uuid_column] = [str(uuid4()) for _ in range(len(df))]
        else:
            rows = df[self.uuid_column].isnull()
            df.loc[rows, self.uuid_column] = [
                str(uuid4()) for _ in range(len(df[rows]))
            ]

        df = df.set_index(self.uuid_column)
        df.to_sql(self.table_name, dbcon, if_exists="replace")

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

        with sqlite3.connect(self.sqlite_path) as dbcon:
            dbcon.execute(
                f'UPDATE "{self.table_name}" SET "{col}" = ? WHERE "{self.uuid_column}" = ?',
                (value, row),
            )

    async def arun(
        self,
        sqlite_path: str,
        table_name: str,
        continue_cols: List[str],
        force_rerun: bool = False,
        uuid_column: str = ":uuid:",
        retval_column: Optional[str] = ":retval:",
        extra_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Async run experiments from a csv file"""

        self.sqlite_path = sqlite_path
        self.table_name = table_name
        self.continue_cols = continue_cols
        self.uuid_column = uuid_column
        self.extra_kwargs = extra_kwargs or {}

        with sqlite3.connect(sqlite_path) as dbcon:

            tasks = self.submit_from(dbcon, force_rerun)

            # block until all tasks are done
            for task in asyncio.as_completed(tasks):
                exp, results = await task
                logger.info(f"Finished {exp.uuid}")
                if retval_column is not None:
                    await self._write_cell(exp.uuid, retval_column, results)
