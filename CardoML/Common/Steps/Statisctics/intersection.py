from typing import List

from CardoExecutor.Common.CardoContext import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class Intersection(IStep):
	"""
	log ids intersection counts with another dataset to elastic
	"""

	def __init__(self, ids: List, log_name) -> None:
		self.ids = ids
		self.log_name = log_name

	def process(self, cardo_context: CardoContextBase, dataframe: CardoDataFrame,
				intersection_dataset: CardoDataFrame) -> CardoDataFrame:
		intersections = self.__intersect(dataframe, intersection_dataset)
		cardo_context.logger.info(self.log_name, extra={'table_name': intersection_dataset.table_name,
														'statistic_type': self.log_name,
														'statistic_value': intersections})
		return dataframe

	def __intersect(self, dataframe: CardoDataFrame, dataset: CardoDataFrame) -> int:
		intersect_count = dataframe.dataframe.join(dataset.dataframe, on=self.ids).count()
		return intersect_count
