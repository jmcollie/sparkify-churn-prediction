import seaborn as sns
import seaborn.objects as so
from matplotlib import pyplot as plt




class Plotter():
    """
    A class for creating plots using `seaborn`
    functions and `seaborn.objects`.
    
    Parameters
    ----------
    query : str
        Query string for selecting the dataset.
    palette : str 
        Color palette.
    rc_params : dict or None
        Dictionary of rc parameter mappings.
    
    Attributes
    ----------
    data : `pandas.DataFrame`
        A tabular dataset used for plotting.
    palette : str or sequence
        Color palette.
    rc_params : dict or None
        Dictionary of rc parameter mappings.
    """
    
    def __init__(self, query, palette, rc_params: dict):
        self.data = spark.sql(query).toPandas()
        self.set_theme(palette, rc_params)
    
    
    def set_theme(self, palette, rc_params):
        """
        Sets visual aspects used by plots.
        
        Parameters
        ----------
        
        palette : str or sequence
            Color palette.
        
        rc_params : dict
            Dictionary of rc parameter mappings.
        
        Returns
        -------
        None
        """
        
        self.palette = palette
        self.rc_params = rc_params
        
        sns.set_theme(
            palette=self.palette, 
            rc=self.rc_params
        )

    
    def plot(self, func = None, plot_kw: dict = None):
        """
        Creates a plot using `seaborn` functions or
        `seaborn.objects.Plot` interface. 
        
        
        Parameters
        ----------
        func : str, default = None
            The `seaborn` function name or None.
            
        plot_kw : dict, default=None
            A dictionary with keywords passed to the `seaborn` function
            call or `seaborn.objects.Plot` constructor.
        
        Returns
        -------
        Returns the Axes object with the plot drawn onto it when a function from
        `seaborn` is specified. When parameter `func` is None, returns
        an instance of `seaborn.objects.Plot`. 
        """
        if func is None:
            return (
                so.Plot(
                    self.data,
                    **plot_kw
                )
                .theme(sns.axes_style())
                .scale(color=self.palette)
            )
        
        elif hasattr(sns, func) and callable(plot = getattr(sns, func)):
            return plot(
                self.data,
                **plot_kw
            )

            
    @staticmethod
    def create_subplots(subplot_kw: dict):
        """
        Creates a figure with subplots.
        
        Parameters
        ----------
        subplot_kw : dict
            Dictionary with keywords passed to the 
            `matplotlib.pyplot.subplots` call.
            
        Returns
        -------
        fig
            `matplotlib.figure.Figure`.
        
        ax
            `matplotlib.axes.Axes`, or an array 
            of `matplotlib.axes.Axes`
        """
        return plt.subplots(
            **subplot_kw
        )
    
    
class Eda(Plotter):
    """
    A class for performing exploratory data analysis
    on the Sparkify dataset. 
    """
    
    def compare_prop_by_status(self, column: str):
        """
        Creates a histogram for comparing the proportion
        of users for each `column` value by churn status.
        
        Parameters
        ----------
        column : str
            The name of the column to plot.
        
        Returns
        -------
        None
        
        
        """
        (
            self.plot(
                plot_kw = dict(
                    x=column,
                    color='status'
                )
            )
            .add(so.Bar(), so.Hist(stat='proportion'), so.Dodge(gap=.1))
            .label(
                x=column.replace('_', ' '),
                y='proportion of users',
                title='{} By Status'.format(column.replace('_', ' ').title()) 
            )
            .layout(engine='tight')
            .on(plt.figure())
            .show()
        )
    
    
    def compare_dist_by_status(self, column: str):
        """
        Creates a boxplot and histogram for comparing the distribution of
        of `column` values by churn status.
        
        
        Parameters
        ----------
        column : str
            The name of the column to plot.
        
        Returns
        -------
        None
        
        """
        fig, (ax_box, ax_hist) = self.create_subplots(
            dict(
                nrows=2, 
                sharex=True, 
                gridspec_kw={"height_ratios": (.25, .75)}
            )
        )


        (
            self.plot(
                func='boxplot',
                plot_kw=dict(
                    x=column, 
                    y='status', 
                    ax=ax_box,
                    notch=True
                )
            )
            .set(
                title='{} By Status'.format(
                    column.replace('_', ' ').title()
                ),
                xlabel=None
            )
        )


        (
            self.plot(
                plot_kw=dict(
                    x=column, 
                    color='status'
                )
            )
            .add(so.Bars(), so.Hist(stat='proportion'))
            .label(
                x=column.replace('_', ' '),
                y='proportion of users',
            )
            .on(ax_hist)
            .show()
        )