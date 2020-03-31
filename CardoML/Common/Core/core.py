import asyncio
import functools
from threading import Lock
from typing import Any, Callable, Generator, List, Union, Tuple

import numpy as np
import pandas as pd
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoLibs.IO import HiveWriter, HiveReader
from pyspark.sql.catalog import Catalog

from .config import LOCK_ATTRIBUTE, DATA_ATTRIBUTE


def union_dataframes(*dataframes: Union[CardoDataFrame, List[CardoDataFrame], Tuple[CardoDataFrame]]):
    """
    :param dataframes:
    :return: union of all those dataframes
    """
    if isinstance(dataframes[0], list) or isinstance(dataframes[0], tuple):
        return union_dataframes(*[dataframe for many_dataframe in dataframes for dataframe in many_dataframe])

    else:
        if dataframes[0].payload_type in ['dataframe', 'rdd']:
            unioned = functools.reduce(
                lambda df1, df2: CardoDataFrame(df1.dataframe.union(df2.dataframe.select(df1.dataframe.columns))),
                dataframes)
            return unioned
        if dataframes[0].payload_type == 'pandas':
            unioned = pd.concat(dataframes, axis=0)
            return unioned


def generic_fillna(df: pd.DataFrame, fill_zeros: bool=True, value: Any=np.nan, *args, **kwargs) -> pd.DataFrame:
    default_fill = {None: np.nan, 'nan': np.nan, 'None': np.nan, 'none': np.nan,
                    'Nan': np.nan, 'NAN': np.nan, '': np.nan}
    if fill_zeros:
        default_fill.update({'0': np.nan})
    return df.replace(default_fill).fillna(*args, value=value, **kwargs)


def get_all_subclasses(cls) -> Generator[Callable, Any, None]:
    """
    :param cls:
    :return: all all classes that somewhere along the way inherit of the class
    """
    for a_class in cls.__subclasses__():
        yield a_class
        for b_class in get_all_subclasses(a_class):
            yield b_class


def lock(lock_attribute: str=LOCK_ATTRIBUTE) -> Callable:
    """
    Add a lock mechanism to the class if its not already exists.
    Use that lock whenever the function is called.
    """
    def wrapper(func: Callable):
        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            if not hasattr(self, lock_attribute):
                setattr(self, lock_attribute, Lock())
            with getattr(self, lock_attribute):
                return func(self, *args, **kwargs)
        return inner
    return wrapper


def register(attribute_name: str=DATA_ATTRIBUTE) -> Callable:
    """
    Assign the return value of a function into the attribute_name attribute of a class if it doesn't exist already.
    Return the assigned value whenever the function is called again.
    If the attribute_name already exists it will just return it.
    """
    def wrapper(func: Callable):
        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            if not hasattr(self, attribute_name):
                setattr(self, attribute_name, func(self, *args, **kwargs))
            return getattr(self, attribute_name)
        return inner
    return wrapper


def persist(func: Callable) -> Callable:
    """
    :param func: any process function that have CardoContext input and returns CardoDataFrame
    :return: persisted CardoDataFrame
    """
    @functools.wraps(func)
    def inner(self, cardo_context: CardoContextBase, *args, **kwargs):
        return func(self, cardo_context, *args, **kwargs).persist()
    return inner


def snake_case(camel_case: str) -> str:
    """
    :param camel_case: CamelCase string
    :return: snake_case string
    """
    tmp = pd.Series(list(camel_case))
    query = (tmp.str.isupper()) & (tmp.index > 0)
    tmp[query] = tmp[query].apply(lambda s: "_" + s)
    return ''.join(tmp.str.lower().tolist())


def async_retry_function(retries: int, sleep_for: int) -> Callable:
    def inner(func: Callable):
        async def f_retry(*args, **kwargs):
            mtries = retries
            while mtries > 0:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    await asyncio.sleep(sleep_for)
                    print("RETRY! exception: ", e, *args, kwargs)
                    mtries -= 1
            raise Exception("exception in `async_retry_function`, exceeded maximum allowed failures")
        return f_retry
    return inner


def async_wrapper(func: Callable) -> Callable:
    @functools.wraps(func)
    async def inner(self, *args, **kwargs):
        return await asyncio.get_event_loop().run_in_executor(None, functools.partial(func, **kwargs), self, *args)
    return inner


def chunker(values: List, chunksize: int) -> Generator[List, Any, None]:
    for i in range(0, len(values), chunksize):
        yield values[i:i + chunksize]


def reuse_last_run(full_table_name: str) -> Callable:
    """
    Usage Examples:
        1)
        class ExampleStep(IStep):
            @reuse_last_run("default.backup")
            def process(self, cardo_context, cardo_dataframe):
                ...
                return cardo_dataframe

        2)
        class MyFunc(FuncSimilarity):
            @reuse_last_run("default.func_backup")
            def process(self, cardo_context, cardo_dataframe):
                ...
                return super().process(self, cardo_context, cardo_dataframe)

        3)
            a_step = MyStep(...)
            new_process = reuse_last_run("default.backup")(MyStep.process)
            new_process(a_step, context, cardo_dataframe)

    :param full_table_name: the name of the hive schema and table in this format: "schema.table"
    """
    schema_name, table_name = full_table_name.split('.')

    def wrapper(func: Callable):
        @functools.wraps(func)
        def inner(self: IStep, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame) -> CardoDataFrame:
            df = cardo_dataframe.dataframe
            last_run = None
            if any([table_name == table.name for table in Catalog(cardo_context.spark).listTables(schema_name)]):
                last_run = cardo_context.spark.table(full_table_name)
                df = df.join(last_run, on=df.columns, how='left_anti')

            current_results = func(self, cardo_context, CardoDataFrame(df, cardo_dataframe.table_name))

            if not last_run:
                HiveWriter(full_table_name).process(cardo_context, current_results)
            else:
                HiveWriter(full_table_name, mode='append').process(cardo_context, current_results)

            cardo_dataframe.dataframe = df.join(HiveReader(full_table_name).process(cardo_context).dataframe,
                                                on=df.columns)
            return cardo_dataframe
        return inner
    return wrapper
