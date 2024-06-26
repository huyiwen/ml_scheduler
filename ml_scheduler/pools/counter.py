from typing import Optional

from .base import BaseElement, BasePool


class CounterPool(BasePool):

    def __init__(
        self,
        size: int,
        available: Optional[int],
    ):
        self.size = size
        available = available if available is not None else size
        self.pool = [BaseElement(1, i >= available) for i in range(size)]
