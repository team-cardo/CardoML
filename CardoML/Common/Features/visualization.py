import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import norm
import scipy
import seaborn as sns


def feature_similarity(features: pd.DataFrame, figsize: tuple = (16, 10), font_size: int = 15) -> None:
    """
    Plot an hierarchy graph that visualise the similarity of columns
    """
    # not as good for continuous features, find a way to do it by column type
    corr = np.round(scipy.stats.spearmanr(features).correlation, 4)
    corr_condensed = scipy.cluster.hierarchy.distance.squareform(1 - corr)
    z = scipy.cluster.hierarchy.linkage(corr_condensed, method='average')
    fig = plt.figure(figsize=figsize)
    dendrogram = scipy.cluster.hierarchy.dendrogram(z, labels=features.columns, orientation='left',
                                                    leaf_font_size=font_size)
    plt.show()
    return


def describe_col(df: pd.DataFrame, col: str):
    """
    blue line = gaussian distribution
    black line = normal probability
    """
    sns.distplot(df[col], fit=norm)
    plt.show()
    scipy.stats.probplot(df[col], plot=plt)
    plt.show()
    print(f"Skewness: {df[col].skew()}")
    print(f"Kurtosis: {df[col].kurt()}")


def find_outliers(df: pd.DataFrame, col: str, label: str, plot_type: str = 'box', figsize: tuple = (20, 10)):
    """
    :param df:
    :param col:
    :param label:
    :param plot_type: box or scatter
    :param figsize:
    :return:
    """
    plt.figure(figsize=figsize)
    data = pd.concat([df[col], df[label]], axis=1)

    if plot_type == 'box':
        sns.boxplot(x=col, y=label, data=data)
    if plot_type == 'scatter':
        data.plot.scatter(x=col, y=label, figsize=figsize)

    plt.xticks(rotation=90)
    plt.show()


def heatmap(df: pd.DataFrame, show_num: bool = True, figsize: tuple = (12, 9)):
    corr = df.corr()
    plt.figure(figsize=figsize)
    sns.heatmap(corr, annot=show_num, fmt='.2f')
    plt.show()


def compare_labeled_data(df: pd.DataFrame, label_col: str, plot_type: str = 'violin', figsize: tuple = (12, 9)):
    """
    :param df:
    :param label_col:
    :param plot_type: violin or box
    :param figsize:
    :return:
    """
    data = pd.melt(df, id_vars=label_col, var_name='features', value_name='value')
    plt.figure(figsize=figsize)
    if plot_type == 'violin':
        sns.violinplot(x='features', y='value', hue=label_col, data=data, split=True, inner='quart')
    elif plot_type == 'box':
        sns.boxplot(x='features', y='value', hue=label_col, data=data)
    plt.xticks(rotation=45)
    plt.show()
