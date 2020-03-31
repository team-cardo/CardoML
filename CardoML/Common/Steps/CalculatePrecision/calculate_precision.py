from typing import Tuple

import pyspark.sql.dataframe as SparkDataFrame
import pyspark.sql.functions as F
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from pyspark.sql.window import Window


class CalculatePrecision(IStep):
	"""
	Parameters
	----------
	intersection_column : str
		column to join the dataframes on.

	match_column : str
		column to match the values on, precision base on it.

	friendly_precision : bool, default False
		whether matching only one of the possible `match_column` is enough for the whole group to be true.

	source_name : str, default None
		the source name for the logs. if none is given use the `table_name` property of the CardoDataFrame

	precision_column : str, default None
		if given keep the precision calculated as a column under the name given.

	log_type : str, default `visualize`
		the name of the `log_type` field in the logger, can used to filter logs for visualization
	"""
	def __init__(self,
				 intersection_column: str,
				 match_column: str,
				 source_name: str = None,
				 friendly_precision: bool = False,
				 precision_column: str = None,
				 log_type: str = 'visualize'):
		self.intersection_column = intersection_column
		self.match_column = match_column
		self.precision_column = precision_column
		self.friendly_precision = friendly_precision
		self.source_name = source_name
		self.log_type = log_type

	def __get_intersections(self, dataframe: SparkDataFrame, ground_truth: SparkDataFrame) -> Tuple[int, int]:
		tmp_label = 'tmp_label'
		intersected = dataframe.select(self.intersection_column,
									   self.match_column
									   ).join(ground_truth.select(self.intersection_column,
																  self.match_column), self.intersection_column)
		intersected = intersected.withColumn(tmp_label, F.when(dataframe[self.match_column] ==
															   ground_truth[self.match_column], 1).otherwise(0))
		if self.friendly_precision:
			intersected = intersected.withColumn(tmp_label,
												 F.max(tmp_label).over(Window.partitionBy(self.intersection_column)))

		intersected.persist()
		all_positives_count = intersected.count()
		true_positive_count = intersected.filter(F.col(tmp_label) == 1).count()
		intersected.unpersist()
		return true_positive_count, all_positives_count

	@staticmethod
	def __get_precision_value(true_positive_count: int, all_positives_count: int) -> float:
		return 0 if all_positives_count == 0 else true_positive_count / all_positives_count

	def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame,
				gt: CardoDataFrame) -> CardoDataFrame:
		dataframe = cardo_dataframe.dataframe
		ground_truth_dataframe = gt.dataframe
		true_positive_count, all_positives_count = self.__get_intersections(dataframe, ground_truth_dataframe)
		precision_value = self.__get_precision_value(true_positive_count, all_positives_count)
		if self.precision_column:
			cardo_dataframe.dataframe = dataframe.withColumn(self.precision_column, F.lit(precision_value))
		source_name = self.source_name if self.source_name else cardo_dataframe.table_name
		cardo_context.logger.info(f"precision calculation for {source_name} -> "
								  f"matches: {true_positive_count}, intersection: {all_positives_count}, "
								  f"precision: {precision_value}, "
								  f"is friendly: {self.friendly_precision}",
								  extra={"gt_match": true_positive_count,
										 "id": f"{self.match_column}_for_{self.intersection_column}_{gt.table_name}",
										 "log_type": self.log_type,
										 "count": all_positives_count,
										 "statistic_value": precision_value,
										 "statistic_type": "precision",
										 "table_name": source_name,
										 "base_table": gt.table_name})
		return cardo_dataframe
