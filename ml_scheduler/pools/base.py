import asyncio
from functools import cached_property
from typing import Generic, List, Type, TypeVar


class BaseElement:

    def __init__(self, size: int, allocated: bool):
        self.size = size
        self.allocated = allocated

    def is_allocated(self) -> bool:
        return self.allocated

    async def cleanup(self):
        self.allocated = False

    def allocate(self):
        """Pre consume the resource."""
        if self.allocated:
            return []
        self.allocated = True
        return [self]

    def is_unavailable(self) -> bool:
        return self.is_allocated()


T_co = TypeVar('T_co', bound=BaseElement)


class BaseResources(List[T_co]):

    def size(self):
        return sum(res.size for res in self if res.is_allocated())

    async def cleanup(self):
        for res in self:
            await res.cleanup()

    def __hash__(self) -> int:
        return hash(tuple(self))


class BaseAllocator(Generic[T_co]):

    pool: "BasePool"

    def __init__(self, pool: "BasePool"):
        self.pool = pool

    async def _get_size(self, size: int):
        """Get the size of the resources to allocate. Share the same signature as `_callback` except for the first argument."""
        return size

    async def _callback(self, _allocated: BaseResources, *args, **kwargs):
        """Do something after the resources are allocated. Share the same signature as `_get_size` except for the first argument."""
        pass

    async def _allocate(self, size: int):
        allocated = []
        for res in self.pool:
            allocated.extend(res.allocate())
            if sum(a.size for a in allocated) >= size:
                break
        return allocated

    async def __call__(self, *args, **kwargs) -> BaseResources[T_co]:
        allocated = BaseResources[T_co]()
        print_after = 5
        interval = 1

        while allocated.size() < (size := await
                                  self._get_size(*args, **kwargs)):
            if print_after == 0:
                print(f"Waiting for {size} {self.pool} resources...")
            if print_after >= 0:
                print_after -= 1
            await asyncio.sleep(interval)
            allocated.extend(await self._allocate(size))

        await self._callback(allocated, *args, **kwargs)
        return allocated


class BasePool:

    element_type: Type[BaseElement] = BaseElement
    pool: List[BaseElement]

    @cached_property
    def allocate(self):
        """Base allocator.

        Args:
            size: The size of the resources to allocate."""
        return BaseAllocator[BaseElement](self)

    def __iter__(self):
        return iter(self.pool)

    @property
    def available_size(self):
        return sum(res.size for res in self.pool if res.is_unavailable())
