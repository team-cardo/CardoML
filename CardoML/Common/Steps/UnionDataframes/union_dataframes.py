from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

from CardoML.Common.Core import union_dataframes


class UnionDataframes(IStep):
    def process(self, cardo_context: CardoContextBase, *cardo_dataframes: CardoDataFrame) -> CardoDataFrame:
        return union_dataframes(cardo_dataframes)
