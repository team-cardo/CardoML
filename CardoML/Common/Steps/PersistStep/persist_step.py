from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

from CardoML.Common.Core import persist


class PersistStep(IStep):
    node_attributes = {'shape': 'rectangle'}

    @persist
    def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame) -> CardoDataFrame:
        return cardo_dataframe
