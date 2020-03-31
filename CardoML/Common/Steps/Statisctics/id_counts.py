from CardoExecutor.Common.CardoContext import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep


class IdCounts(IStep):
	"""
	log id counts to elastic
	"""

	def __init__(self, id: str):
		self.id = id

	def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame) -> CardoDataFrame:
		self.__count_per_logic(cardo_context, cardo_dataframe)
		self.__num_of_distinct_ids_per_logic(cardo_context, cardo_dataframe, self.id)
		return cardo_dataframe

	@staticmethod
	def __count_per_logic(cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame) -> None:
		ids_count = cardo_dataframe.dataframe.count()
		cardo_context.logger.info('id_counts', extra={'table_name': cardo_dataframe.table_name,
													  'statistic_type': 'id_counts',
													  'statistic_value': ids_count})

	@staticmethod
	def __num_of_distinct_ids_per_logic(cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame,
										id_name: str) -> None:
		ids_distinct_count = cardo_dataframe.dataframe.select(id_name).distinct().count()
		cardo_context.logger.info('id_distinct_counts', extra={'table_name': cardo_dataframe.table_name,
															   'statistic_type': id_name,
															   'statistic_value': ids_distinct_count})
