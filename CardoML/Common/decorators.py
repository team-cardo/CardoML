from datetime import datetime
from subprocess import Popen, PIPE

import pandas as pd
from CardoLibs.IO import HdfsWriter, HdfsReader


def save_data(step):
    def inner(self, cardo_context, *args):
        project_name = cardo_context.spark.sparkContext.appName
        run_id = cardo_context.run_id
        step_name = self.__class__.__name__
        saved_df = []
        no_names_index = 0

        for dataframe in args:
            df_name = dataframe.table_name
            if (df_name == '') or (' ' in df_name):
                df_name = no_names_index
                no_names_index += 1
            if dataframe.payload_type in ['dataframe', 'rdd']:
                path = '{}/{}/{}/{}'.format(project_name, step_name, df_name, run_id)
                HdfsWriter(path, 'overwrite', 'parquet').process(cardo_context, dataframe)
                cardo_context.logger.info('save dataframe:{} to path:{}'.format(df_name, path))
                dataframe = HdfsReader(path, 'parquet').process(cardo_context)
                dataframe.table_name = df_name
                saved_df.append(dataframe)
            elif dataframe.payload_type == 'pandas':
                path = '{}/{}/{}/{}'.format(project_name, step_name, df_name, run_id)
                HdfsWriter(path, 'overwrite', 'parquet').process(cardo_context, dataframe)
                cardo_context.logger.info('save dataframe:{} to path:{}'.format(df_name, path))
                saved_df.append(dataframe)
            else:
                saved_df.append(dataframe)
        return step(self, cardo_context, *saved_df)

    return inner


def lazy(compute, user, max_days_diff=0):
    def actual_decorator(step):
        def inner(self, cardo_context, *args):
            project_name = cardo_context.spark.sparkContext.appName
            run_id = cardo_context.run_id
            step_name = self.__class__.__name__
            no_names_index = 0
            saved_df = []
            path = ''
            for dataframe in args:
                if user is not None:
                    path = '/user/{}/'.format(user)

                if dataframe.payload_type in ['dataframe', 'rdd']:
                    df_name = dataframe.table_name
                    if (df_name == '') or (' ' in df_name):
                        df_name = no_names_index
                        no_names_index += 1

                    df_path = path + '{}/{}/{}'.format(project_name, step_name, df_name)
                    last_date, last_folder = __get_hdfs_last_date(df_path)
                    days_diff = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d %H:%M')).days

                    if compute and ((days_diff > max_days_diff) or max_days_diff == 0):
                        df_run_path = '{}/{}'.format(df_path, run_id)
                        HdfsWriter(df_run_path, 'overwrite', 'parquet').process(cardo_context, dataframe)
                    elif not compute:
                        last_date, _ = __get_hdfs_last_date(df_path)

                    _, last_folder = __get_hdfs_last_date(df_path)
                    dataframe = HdfsReader(last_folder, 'parquet').process(cardo_context)
                    dataframe.table_name = df_name
                    saved_df.append(dataframe)

                elif dataframe.payload_type == 'pandas':
                    df_name = dataframe.table_name
                    if (df_name == '') or (' ' in df_name):
                        df_name = no_names_index
                        no_names_index += 1

                    df_path = path + '{}/{}/{}'.format(project_name, step_name, df_name)
                    last_date, last_folder = __get_hdfs_last_date(df_path)
                    days_diff = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d %H:%M')).days

                    if compute and ((days_diff > max_days_diff) or max_days_diff == 0):
                        df_run_path = '{}/{}'.format(df_path, run_id)
                        HdfsWriter(df_run_path, 'overwrite', 'parquet').process(cardo_context, dataframe)
                    elif not compute:
                        last_date, _ = __get_hdfs_last_date(df_path)

                    _, last_folder = __get_hdfs_last_date(df_path)
                    dataframe = pd.read_parquet('/mnt/hadoopnfs{}'.format(last_folder))
                    dataframe.table_name = df_name
                    saved_df.append(dataframe)
                else:
                    saved_df.append(dataframe)
            return step(self, cardo_context, *saved_df)

        return inner

    return actual_decorator


def __get_hdfs_last_date(project_name):
    out, _ = Popen('hdfs dfs -ls {}/ | sort -k6,7'.format(project_name), stdout=PIPE, shell=True).communicate()
    files = out.split()
    if len(files) >= 3:
        last_folder = files[-1]
        hour = files[-2]
        day = files[-3]
        last_date = '{} {}'.format(day, hour)
        return last_date, last_folder
    return '1970-01-01 00:00', ''
