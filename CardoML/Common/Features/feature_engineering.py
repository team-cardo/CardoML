import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype


def str_to_cats(dataframe: pd.DataFrame):
    """
    Convert string column to categories
    """
    for col_name, col in dataframe.items():
        if is_string_dtype(col):
            dataframe[col_name] = col.astype('category').cat.as_ordered()
    return dataframe


def apply_cats(dataframe: pd.DataFrame, cats_dataframe: pd.DataFrame):
    """
    apply the categories in cats_dataframe to dataframe
    :param dataframe:
    :param cats_dataframe:
    :return:
    """
    for col_name, col in dataframe.items():
        if (col_name in cats_dataframe.columns) and (cats_dataframe[col_name].dtype.name == 'category'):
            dataframe[col_name] = col.astype('category').cat.as_ordered()
            dataframe[col_name] = col.cat.set_categories(cats_dataframe[col_name].car.categories, ordered=True)
    return dataframe


def cats_to_codes(dataframe: pd.DataFrame, max_n_cats: int = None):
    """
    converst categories to ints
    :param dataframe:
    :param max_n_cats: converts categories to int only if there are less than max_n_cats different categories on the column
    :return:
    """
    for col_name, col in dataframe.items():
        if not is_numeric_dtype(col) and (max_n_cats is None or len(col.cat.categories) > max_n_cats):
            dataframe[col_name] = pd.Categorical(col).codes + 1  # nulls are -1 so with +1 they are 0
    return dataframe


def show(df: pd.DataFrame, max_rows: int = 1000, max_columns: int = 1000, col_width: int = -1):
    try:
        from IPython import get_ipython
        from IPython.display import display
        get_ipython()
        with pd.option_context("display.max_rows", max_rows, 'display.max_columns', max_columns, 'display.max_colwidth',
                               col_width):
            display(df)
    except:
        print(df)


def fix_gt_ratio(data: pd.DataFrame, gt: pd.DataFrame, labels: list):
    raise NotImplementedError
