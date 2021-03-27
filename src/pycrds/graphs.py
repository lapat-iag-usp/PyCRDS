from bokeh.plotting import figure, show


def bokeh_graph(df, start_time, end_time, var):
    """
    Returns a bokeh graph of one variable for the selected period.

    Parameters:
        df (pandas DataFrame): dataframe
        start_time (str) : 'yyyy-mm-dd hh:mm:ss'
        end_time (str) : 'yyyy-mm-dd hh:mm:ss'
        var (str): selected variable
    """
    df = df.loc[(df.index >= start_time) & (df.index < end_time)]
    p = figure(x_axis_type="datetime", plot_width=750, plot_height=250, toolbar_location="above")
    p.line(df.index, df[var].values, line_width=1.5, color='#2ca02c')
    p.xaxis.axis_label = 'UTC'
    p.yaxis.axis_label = var
    #     p.xaxis.major_label_orientation = pi/8
    #     p.xaxis.formatter=DatetimeTickFormatter(
    #             years=["%Y-%m-%d %H:%M:%S"],
    #             months=["%Y-%m-%d %H:%M:%S"],
    #             days=["%Y-%m-%d %H:%M:%S"],
    #             hours=["%Y-%m-%d %H:%M:%S"],
    #             minutes=["%Y-%m-%d %H:%M:%S"],
    #             seconds=["%Y-%m-%d %H:%M:%S"])
    return show(p)
