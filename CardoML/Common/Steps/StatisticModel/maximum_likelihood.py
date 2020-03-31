import math

import pyspark.sql.functions as F
from CardoExecutor.Common.CardoContext import CardoContextBase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStep import IStep
from pyspark.sql import dataframe

from CardoML.Common.Core.core import union_dataframes


class MaximumLikelihood(IStep):
    def __init__(self, group_by_cols, grade_cols, aggregations, log_value_for_ones=math.log(0.00001), allow_ones=False,
                 replace_ones=0.9999, concat_ws_seperator=', '):
        # type: (list,list,dict,float,bool,bool,str) -> None
        self.group_by_cols = group_by_cols
        self.grade_cols = grade_cols
        self.aggregations = aggregations
        self.log_value_for_ones = log_value_for_ones
        self.allow_ones = allow_ones
        self.replace_ones = replace_ones
        self.concat_ws_seperator = concat_ws_seperator

    def process(self, cardo_context, *dataframes):
        # type: (CardoContextBase,list) -> CardoDataFrame
        df = union_dataframes(dataframes)
        df = df.dataframe

        df = self.get_log_of_grades(df)
        df = self.combine_sources(df)
        df = self.convert_grade_back_to_normal(df)
        if not self.allow_ones:
            df = self.fix_ones(df)

        return CardoDataFrame(df)

    def get_log_of_grades(self, df):
        # type: (dataframe) -> dataframe
        for col in self.grade_cols:
            df = df.withColumn(col, F.coalesce(F.log(F.lit(1) - F.col(col)), F.lit(self.log_value_for_ones)))
        return df

    def combine_sources(self, df):
        # type: (dataframe) -> dataframe
        concat_ws = []
        for col, agg in self.aggregations.items():
            if agg == 'concat_ws':
                concat_ws.append(col)
                self.aggregations[col] = 'collect_set'

        df = df.groupBy(self.group_by_cols).agg(self.aggregations)
        for col, agg in self.aggregations.items():
            df = df.withColumnRenamed("{agg}({col})".format(agg=agg, col=col), col)

        for col in concat_ws:
            df = df.withColumn(col, F.concat_ws(self.concat_ws_seperator, col))
        return df

    def convert_grade_back_to_normal(self, df):
        # type: (dataframe) -> dataframe
        for col in self.grade_cols:
            df = df.withColumn(col, F.lit(1) - F.exp(F.col(col)))
        return df

    def fix_ones(self, df):
        # type: (dataframe) -> dataframe
        for col in self.grade_cols:
            df = df.withColumn(col, F.when(F.col(col) == 1, self.replace_ones).otherwise(F.col(col)))
        return df
