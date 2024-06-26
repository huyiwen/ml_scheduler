from functools import cached_property
from typing import List

from nvitop import Device

from .base import BaseAllocator, BaseElement, BasePool


class CUDAElement(BaseElement):

    def __init__(self, device: Device, min_memory: float = 90):
        super().__init__(1, False)
        self.device = device
        self.min_memory = min_memory
        self.cuda_index = device.cuda_index

    def is_unavailable(self):
        return (100 - self.device.memory_percent()
                ) > self.min_memory or self.is_allocated()

    def __str__(self) -> str:
        return str(self.cuda_index)

    def __repr__(self) -> str:
        return "CUDA(device={}, allocated={}, memory_percent={})".format(
            self.device.cuda_index, self.allocated,
            self.device.memory_percent())


class CUDAPool(BasePool):

    def __init__(self, ids: List[int], min_memory: float = 20):
        devices = Device.cuda.all()
        devices = {d.cuda_index: d for d in devices}
        self.pool = [CUDAElement(devices[id], min_memory) for id in ids]

    @cached_property
    def allocate(self):
        return BaseAllocator[CUDAElement](self)

    def __repr__(self) -> str:
        return f"CUDAPool(avai={self.available_size})"
