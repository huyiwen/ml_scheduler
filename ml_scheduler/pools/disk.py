import asyncio
import os
import shutil
from functools import cached_property
from logging import getLogger
from pathlib import Path
from typing import List, Literal, Optional

import psutil

from ..threads import to_thread
from .base import BaseAllocator, BaseElement, BasePool, BaseResources

logger = getLogger(__name__)


class DiskElement(BaseElement):

    def __init__(self, size: int, source_folder: str, target_folder: str,
                 cleanup_target: bool, disk_allocator: "DiskAllocator"):
        self.size = size
        self.source_folder = source_folder
        self.target_folder = target_folder
        self.cleanup_target = cleanup_target
        self.disk_allocator = disk_allocator
        self.allocated = True

    async def cleanup(self):
        self.allocated = False
        if self.cleanup_target:
            logger.info(f"Cleaning up {self.target_folder}")
            await to_thread(shutil.rmtree, self.disk_allocator.target_folder)


class DiskAllocator(BaseAllocator[DiskElement]):

    pool: "DiskPool"
    source_folder: str
    target_folder: str
    cleanup_target: bool = True

    async def _allocate(self, size: int):
        if size > self.pool.available_size:
            return []

        self.pool.pre_allocated += size
        return [
            DiskElement(size, self.source_folder, self.target_folder,
                        self.cleanup_target, self)
        ]


class CopyAllocator(DiskAllocator):

    def __init__(self, pool, unit, max_copys=2):
        super().__init__(pool)
        self.in_copy = 0
        self.max_copys = max_copys
        self.unit = unit

    async def _callback(
        self,
        _allocated: BaseResources,
        source_folder: str,
        target_folder: str,
        files: Optional[List[str]] = None,
        cleanup_target: bool = True,
    ):

        def copy_files(
            source_dir: Path,
            target_dir: Path,
            files: Optional[List[str]] = None,
        ):
            for f in files:
                if not (target_dir / f).exists() or (
                        source_dir / f).stat().st_size != (target_dir /
                                                           f).stat().st_size:
                    src = (source_dir / f).as_posix()
                    tgt = (target_dir / f).as_posix()
                    shutil.copyfile(src, tgt)

        self.source_folder = source_folder
        self.target_folder = target_folder
        self.cleanup_target = cleanup_target
        source_dir = Path(source_folder)
        target_dir = Path(target_folder)
        if files is None:
            files = [
                f for f in os.listdir(source_folder)
                if os.path.isfile(source_dir / f)
            ]

        while self.in_copy >= self.max_copys:
            await asyncio.sleep(1)

        self.in_copy += 1
        await to_thread(copy_files, source_dir, target_dir, files)
        self.pool.pre_allocated -= _allocated.size()
        self.in_copy -= 1

    async def _get_size(
        self,
        source_folder: str,
        target_folder: str,
        files: Optional[List[str]] = None,
        cleanup_target: bool = True,
    ):
        source_dir = Path(source_folder)
        if files is None:
            files = [
                f for f in os.listdir(source_folder)
                if os.path.isfile(source_dir / f)
            ]

        size = sum((source_dir / f).stat().st_size for f in files) // self.unit

        target_dir = Path(target_folder)
        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)
        else:
            size -= sum(
                (target_dir / f).stat().st_size
                for f in files if (target_dir / f).exists()) // self.unit
        return size


class DiskPool(BasePool):

    unit_mapping = {'GB': 1_000_000_000, 'MB': 1_000_000}

    def __init__(
        self,
        path: str,
        unit: Literal['GB', 'MB'] = 'GB',
        max_copys: int = 2,
    ):
        self.path = path
        self.unit = self.unit_mapping[unit]
        self.pre_allocated = 0
        self.callback_count = 0
        self.max_copys = max_copys

    @cached_property
    def allocate(self):
        return DiskAllocator(self)

    @cached_property
    def copy_folder(self):
        """Base allocator.

        Args:
            source_folder: Source folder to copy files from.
            target_folder: Target folder to copy files to.
            files: List of files to copy. If None, all files in source_folder
                will be copied.
        """
        return CopyAllocator(self, max_copys=self.max_copys, unit=self.unit)

    @property
    def available_size(self) -> int:
        return psutil.disk_usage(
            self.path).free // self.unit - self.pre_allocated
