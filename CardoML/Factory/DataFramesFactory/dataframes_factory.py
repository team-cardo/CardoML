from CardoExecutor.Contract.IStep import IStep

from CardoML.Common.Core import get_all_subclasses
from CardoML.Common.ISteps.ICacheStep import ICacheStep
from CardoML.Factory.MetaClasses import Singleton


class DataFramesFactory(metaclass=Singleton):
    """
    Make steps that inherit ICacheStep to be accessible from everywhere (same instance)
    """
    def __init__(self):
        for cached_step in set(get_all_subclasses(ICacheStep)):
            self.__setattr__(cached_step.__name__, cached_step())

    def __getattr__(self, key: str) -> ICacheStep:
        return super().__getattribute__(key.lower())

    def __setattr__(self, key: str, value: IStep) -> None:
        super().__setattr__(key.lower(), value)
