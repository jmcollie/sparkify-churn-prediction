import seaborn as sns
import seaborn.objects as so
from matplotlib import pyplot as plt



rc_params = {
    'font.family': 'sans-serif',
    'figure.titlesize': 'large',
    'figure.titleweight': 'normal',
    'figure.dpi': '120',
    'figure.edgecolor': 'white',
    'figure.facecolor': 'white',
    'axes.titlesize': 'medium',
    'axes.labelsize': '12',
    'axes.facecolor': 'white',
    'axes.titlecolor': 'black',
    'axes.edgecolor': 'white',
    'axes.spines.left': 'True',
    'axes.spines.bottom': 'True',
    'axes.spines.top': 'False',
    'axes.spines.right': 'False',
    'xtick.color': '4C4C4C',
    'xtick.labelsize': '12',
    'ytick.labelsize': '12',
    'ytick.color': '4C4C4C',
    'boxplot.whiskerprops.color': 'red',
    'boxplot.whiskerprops.linewidth': '1',
    'boxplot.capprops.color': '3C3C3C',
    'boxplot.capprops.linewidth': '1',
    'boxplot.boxprops.color': '3C3C3C',
    'boxplot.boxprops.linewidth': '1',
    'boxplot.medianprops.color': '3C3C3C',
    'boxplot.medianprops.linewidth': '1',
    'lines.linewidth': '1',
    'lines.markersize': '10',
    'grid.color': '#D3D3D3',
    'savefig.facecolor': 'white'
}


def box_and_hist_plot_by_status(data, column: str):
    """Creates a boxplot and histplot using 
    `column` grouped by ``status`` column.
    
    
    Parameters
    ----------
    column : str
        The column to used for creating a box and
        hist plot.
        
    data : pyspark.sql.dataframe.DataFrame
        A spark DataFrame containing the column to plot.
    
    Returns
    -------
    None
    """
    
    data = data.select(
        ['status', column]
    ).toPandas()
    
    fig, (ax_box, ax_hist) = plt.subplots(
        nrows=2, 
        sharex=True, 
        gridspec_kw={"height_ratios": (.25, .75)}
    )
    
    sns.boxplot(
        data=data,
        x=column,
        y='status',
        ax=ax_box,
        notch=True
    ).set(
        title='{} By Status'.format(
            column.replace('_', ' ').title()
        ),
        xlabel=None
    )

    sns.histplot(
        data=data, 
        x=column, 
        hue='status',
        stat='proportion', 
        ax=ax_hist
    ).set(
        xlabel=column.replace('_', ' '),
        ylabel='proportion of users'
    )
    plt.tight_layout()
    
    
    
def bar_plot_by_status(data, column: str, order: list=None):
    """Creates a barplot using `column`
    grouped by the ``status`` column.
    
    
    Parameters
    ----------
    column : str
        The column to used for creating a bar plot.
        
    data : pyspark.sql.dataframe.DataFrame
        A spark DataFrame containing the column to plot.
    
    order : list
        A list for modifying the order of the bars.
    
    Returns
    -------
    None
    
    """
    
    fig, ax = plt.subplots()
    
    data = data.select(
        ['status', column]
    ).toPandas()
    
    if order:
        data[column] = pd.Categorical(data[column], order)
        
    sns.histplot(
        data=data,
        x=column,
        hue='status',
        stat='proportion',
        multiple='dodge',
        shrink=.8,
        ax=ax
    ).set(
        title='{} By Status'.format(
            column.replace('_', ' ').title()
        ),
        xlabel=column.replace('_', ' '),
        ylabel='proportion of users'
    )
    plt.tight_layout()


def get_status_counts_by_column(
        data, 
        column: str, 
        is_datetime: bool=False
    ):
    """Gets value counts of the ``status`` column
    within spark DataFrame `data` when grouped by `column`.
    
    Parameters
    ----------
    data : pyspark.sql.dataframe.DataFrame
        The DataFrame to apply groupby
    
    column : str
        The column to group by.
        
    is_datetime : bool, default=False
        Whether the column is of type datetime
    
    Returns 
    -------
    : pandas.core.series.Series
        Returns a pandas series with `column` as the index
        and the normalized value counts of the ``status`` column
        as the values.
    """
    data = (
        data
        .select(
            [column, 'status']
        )
        .toPandas()
    )
    
    if is_datetime:
        data[column] = pd.to_datetime(data[column])
        grouping_columns = [
            data[column].dt.year,
            data[column].dt.month
        ]
    else:
        grouping_columns = [
            column
        ]
    
    
    return (
        data
        .groupby(
            grouping_columns
        )['status']
        .value_counts(normalize=True)
    )