import argparse 
import matplotlib.collections
import pandas as pd 
from typing import Tuple, Dict, List
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib


def load_data(infile: argparse.FileType, meta: Dict) -> pd.DataFrame: 
    data = pd.read_csv(infile, sep=' ', names=['series', 'x', 'y'], index_col=None)
    # check for NaN in any cells (to see if it's NOT well formed 3-col data)
    # (misses values or entire columns become NaNs in pandas)
    if data.isnull().values.any():
        ## could be well-formed two column data. try parsing as two cols and check for NaNs.
        # print("3-col NaNs found")

        data_old = data
        data['y'] = data['x'] #.drop(['y'], axis=1)
        if data.isnull().values.any():
            ## NaNs found under two column hypothesis. not valid data.
            # print("2-col NaNs found")
            print("ERROR: you must provide valid two or three column data. Invalid parsed data:")
            print(data_old)
            exit(1)
        else:
            ## data is well formed two column data, as far as we can tell. add the x column.
            x = []
            # print(data)
            series_row_counts = dict()
            for index, row in data.iterrows():
                s = row['series']
                if s not in series_row_counts: series_row_counts[s] = 0
                series_row_counts[s] += 1
                x.append(series_row_counts[s])
                # print('row: {}'.format(row))
            data['x'] = x
            # print(data)
            
    if meta: 
        if 'ordering' in meta and len(meta['ordering']) > 0: 
            sort_order = meta['ordering'] + [v for v in data['series'].unique() if v not in meta['ordering']]
            print(f"Reordering rows based on ordering list: {sort_order}")
            data.sort_values(by='series', key=lambda x: x.map({v: i for i, v in enumerate(meta['ordering'])}), inplace=True)
            
        if 'ignore' in meta and len(meta['ignore']) > 0: 
            print(f"Ignoring rows where series is in the ignore list: {meta['ignore']}")
            data = data[~data['series'].isin(meta['ignore'])]
            
        if 'translate' in meta and len(meta['translate']) > 0: 
            for k, v in meta['translate'].items():
                print(f"Translating series '{k}' to '{v}'")
                data['series'] = data['series'].replace(k, v)

    return data 


def calc_figures(data: pd.DataFrame, meta: Dict) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    pivot_table_kwargs = dict()
    pivot_table_kwargs['columns'] = 'series'
    pivot_table_kwargs['index'] = 'x'
    pivot_table_kwargs['values'] = 'y'

    tmean = pd.pivot_table(data, **pivot_table_kwargs, aggfunc='mean')
    tmin = pd.pivot_table(data, **pivot_table_kwargs, aggfunc='min')
    tmax = pd.pivot_table(data, **pivot_table_kwargs, aggfunc='max')
    tstd = pd.pivot_table(data, **pivot_table_kwargs, aggfunc='std')
    tcount = pd.pivot_table(data, **pivot_table_kwargs, aggfunc='count')
    
    if 'ordering' in meta and len(meta['ordering']) > 0: 
        sort_order = meta['ordering'] + [v for v in data['series'].unique() if v not in meta['ordering']]
        
        # translate: 
        sort_order = []
        for o in meta['ordering']: 
            if o in meta.get('translate', dict()): 
                sort_order.append(meta['translate'][o])
            else: 
                sort_order.append(o)
        
        print(f"Reordering columns based on sort_order list: {sort_order}")
        tmean = tmean[sort_order]
        tmin = tmin[sort_order]
        tmax = tmax[sort_order]
        
        try:
            tstd = tstd[sort_order]
        except: 
            print("Warning: tstd does not have the same columns as tmean, so skipping reordering.")
        
        tcount = tcount[sort_order]
    
    # print(tmean)
    return tmean, tmin, tmax, tstd, tcount


def calc_error_bars(tmean: pd.DataFrame, tmin: pd.DataFrame, tmax: pd.DataFrame, tstd: pd.DataFrame, tcount: pd.DataFrame) -> pd.DataFrame: 
    
    # ci_margin = 1.96 * tstd / tcount.pow(0.5)

    tpos_err = tmax - tmean
    tneg_err = tmean - tmin
    
    # tpos_err = tneg_err = ci_margin
    err = [[tneg_err[c], tpos_err[c]] for c in tmean]

    return err

def make_plot_kwargs(args: argparse.Namespace, tmean: pd.DataFrame, err: pd.DataFrame) -> Dict: 
    
    common_args = dict(
        legend=False, 
        title=args.title,
        kind=args.kind,
        figsize=(args.width_inches, args.height_inches),
        linewidth=1.5, 
        logy=args.log_y,
        zorder=10
    )
    
    if args.error_bar_width == 0: 
        pass 
    else: 
        if args.kind == 'bar': 
            common_args.update(dict(
                yerr=err, 
                error_kw=dict(
                    elinewidth=args.error_bar_width,
                    ecolor='red'
                )
            ))
        elif args.kind == 'line': 
            common_args.update(dict(
                yerr=err, 
                ecolor='red', 
                elinewidth=args.error_bar_width,
                # capsize=5, # for some reason this will mess up the markers
            ))
    
    if args.kind == 'bar': 
        common_args.update(dict(
            edgecolor='black',
            width=0.75,
            stacked=args.stacked,
        ))
        
    return common_args

def make_legend_kwargs(args: argparse.Namespace): 
    legend_kwargs = dict(
        title=None, 
        loc='upper center', 
        bbox_to_anchor=(0.5, -0.2),
        ncol=args.legend_columns,
        shadow=True,
        fancybox=True, 
    )
    
    return legend_kwargs

def get_hatch(meta: Dict, series: str, label_idx: int) -> str: 
    mapping = meta.get('patterns', dict())
    if series in mapping: 
        return mapping[series]
    else: 
        patterns = ['x', '/', '//', 'O', 'o', '\\', '\\\\', '.', '+', ' ']
        return patterns[label_idx % len(patterns)]
    
        
def get_color(meta: Dict, series: str, label_idx: int) -> str:
    # you can change the color pallete here
    from palettable.cartocolors.qualitative import Pastel_10 as pallete
    import palettable
    mapping = meta.get('colors', dict())
    if series in mapping: 
        
        # if just a number: 
        if isinstance(mapping[series], int): 
            color_idx = mapping[series]
        else: 
            color_cls = mapping[series]['pallete']
            color_idx = mapping[series]['idx']
            try: 
                pallete = getattr(palettable.colorbrewer.sequential, color_cls)
            except: 
                pallete = getattr(palettable.colorbrewer.qualitative, color_cls)
            return pallete.mpl_colors[color_idx]
        
    else:
        color_idx = label_idx % len(pallete.mpl_colors)
    return pallete.mpl_colors[color_idx]
 

def handle_aesthetics_bar(args: argparse.Namespace, ax: plt.Axes, meta: Dict, tmean: pd.DataFrame, reverse_translate: Dict[str, str]) -> None: 
    bars = ax.patches
    labels = [l for l in tmean.columns for _ in range(len(tmean))] 
    
    for bar, label in zip(bars, labels): 
        label_idx = list(tmean.columns).index(label)
        bar.set_hatch(get_hatch(meta, reverse_translate[label], label_idx))
        bar.set_facecolor(get_color(meta, reverse_translate[label], label_idx))
        
def get_marker(meta: Dict, series: str, label_idx: int) -> str:
    mapping = meta.get('markers', dict())
    if series in mapping:
        return mapping[series]
    else:
        return ['o', 's', 'D', 'x', '^', 'v', '<', '>', 'p', 'P', 'h', 'H', '*', 'X', 'd', '|', '_', '8'][label_idx % 18]

def handle_aesthetics_line(args: argparse.Namespace, ax: plt.Axes, meta: Dict, tmean: pd.DataFrame, reverse_translate: Dict[str, str]) -> None:
    labels = list(tmean.columns)
    i = 0
    for label, line in zip(labels, ax.get_lines()): 
        line_color = get_color(meta, reverse_translate[label], labels.index(label))
        line.set_color(line_color)
        line.set_marker(
            get_marker(
                meta, 
                reverse_translate[label], 
                labels.index(label)
            )
        )
        line.set_linewidth(3)
        line.set_markersize(10)
        
        # error bar colors
        if i < len(ax.containers): 
            errorbar_container = ax.containers[i]
            if isinstance(errorbar_container, matplotlib.container.ErrorbarContainer):
                for component in errorbar_container.lines:
                    if isinstance(component, matplotlib.lines.Line2D): #caps or main linee
                        component.set_color(line_color)
                    elif isinstance(component, tuple): 
                        for subcomponent in component: 
                            if isinstance(subcomponent, matplotlib.lines.Line2D): # caps
                                subcomponent.set_color(line_color)
                            elif isinstance(subcomponent, matplotlib.collections.LineCollection): 
                                subcomponent.set_color(line_color)
                                subcomponent.set_linewidth(1)
        
        i += 1

    

def handle_aesthetics(args: argparse.Namespace, ax: plt.Axes, meta: Dict, tmean: pd.DataFrame, data: pd.DataFrame) -> None:
    
    # ax.ticklabel_format(axis='y', style='sci', scilimits=(-3,3)) 
    
    if 'translate' in meta: 
        reverse_translate = {v: k for k, v in meta['translate'].items()}
    else: 
        reverse_translate = {v: v for v in tmean.columns}
        
    reverse_translate = dict()
    for v in tmean.columns:
        reverse_translate[v] = v
    
    if 'translate' in meta:
        for k, v in meta['translate'].items():
            reverse_translate[v] = k # overwrite if there is a translation
        
    if args.kind == 'bar': 
        return handle_aesthetics_bar(args, ax, meta, tmean, reverse_translate)
    else: 
        return handle_aesthetics_line(args, ax, meta, tmean, reverse_translate)

def set_common_plot_properties(args: argparse.Namespace, ax: plt.Axes) -> None: 
    
    # set y min and y max 
    if args.y_min is not None:
        ax.set_ylim(bottom=args.y_min)
    if args.y_max is not None:
        ax.set_ylim(top=args.y_max)
    
    ## maybe remove y grid

    # print("args.no_y_grid={} args.no_y_minor_grid={}".format(args.no_y_grid, args.no_y_minor_grid))
    if not args.no_y_grid:
        plt.grid(axis='y', which='major', linestyle='-')
        if not args.no_y_minor_grid:
            plt.grid(axis='y', which='minor', linestyle='--')

    ## maybe add all-ticks for logy

    if not args.no_y_minor_ticks:
        if args.log_y:
            ax.yaxis.set_minor_locator(ticker.LogLocator(subs="all"))
        else:
            ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

    ## maybe remove axis tick labels

    if args.no_x_axis:
        plt.setp(ax.get_xticklabels(), visible=False)
    if args.no_y_axis:
        plt.setp(ax.get_yticklabels(), visible=False)

    ## set x axis title

    if args.x_title == "" or args.x_title == None:
        ax.xaxis.label.set_visible(False)
    else:
        ax.xaxis.label.set_visible(True)
        ax.set_xlabel(args.x_title)

    ## set y axis title

    if args.y_title == "" or args.y_title == None:
        ax.yaxis.label.set_visible(False)
    else:
        ax.yaxis.label.set_visible(True)
        ax.set_ylabel(args.y_title)
        

