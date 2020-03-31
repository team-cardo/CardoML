from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from typing import Callable


class DefineStep(IStep):
    """
    First option:
        example_step = DefineStep(lambda cardo_df: cardo_df, name='ExampleStep')

    Second option:
        def example_function(cardo_df):
            return cardo_df
        example_step = DefineStep(example_function)
    """
    def __init__(self, funct: Callable, *args, name: str=None, **kwargs):
        self.function = funct
        self.label = "{}: {}".format(self.__class__.__name__, name if name else funct.__name__)
        self.args = args
        self.kwargs = kwargs

    def process(self, cardo_context: CardoContext, *cardo_dataframe: CardoDataFrame) -> CardoDataFrame:
        return self.function(*cardo_dataframe, *self.args, **self.kwargs)
