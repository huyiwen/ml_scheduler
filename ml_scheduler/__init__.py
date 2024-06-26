"""**ml_scheduler**

A lightweight machine learning experiments scheduler in a few lines of simple Python
"""
__version__ = "1.0.0"
import coloredlogs

coloredlogs.install()

from . import pools
from .exp import Exp, exp_func
from .threads import to_thread

__all__ = "pools", "Exp", "exp_func", "to_thread"
