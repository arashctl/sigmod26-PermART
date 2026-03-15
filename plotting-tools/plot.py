
import matplotlib.pyplot as plt
import matplotlib as mpl
import argparse 
import os 
import sys 
import json 
from plot_tools import tools as plot_tools 
from matplotlib import rcParams

rcParams['pdf.fonttype'] = 42
rcParams['ps.fonttype'] = 42

# Parse arguments 
parser = argparse.ArgumentParser(description='Produce a pandas plot from THREE COLUMN <series> <x> <y> data provided via a file or stdin.')
parser.add_argument('-i', dest='infile', type=argparse.FileType('r'), default=sys.stdin, help='input file containing lines of form <series> <x> <y>; if none specified then will use stdin. (if your data is not in this order, try using awk to easily shuffle columns...)')
parser.add_argument('-o', dest='outfile', type=argparse.FileType('w'), default='out.png', help='output file with any image format extension such as .png or .svg; if none specified then plt.show() will be used')
parser.add_argument('-k', dest='kind', default='bar', help='Kind of plot to produce (bar/line); default is bar; see pandas.DataFrame.plot() documentation for other options')
parser.add_argument('-t', dest='title', default="", help='title string for the plot')
parser.add_argument('--x-title', dest='x_title', default="", help='title for the x-axis')
parser.add_argument('--y-title', dest='y_title', default="", help='title for the y-axis')
parser.add_argument('--width', dest='width_inches', type=float, default=8, help='width in inches for the plot (at given dpi); default 8')
parser.add_argument('--height', dest='height_inches', type=float, default=6, help='height in inches for the plot (at given dpi); default 6')
parser.add_argument('--dpi', dest='dots_per_inch', type=int, default=100, help='DPI (dots per inch) to use for the plot; default 100')
parser.add_argument('--no-x-axis', dest='no_x_axis', action='store_true', help='disable the x-axis')
parser.set_defaults(no_x_axis=False)
parser.add_argument('--no-y-axis', dest='no_y_axis', action='store_true', help='disable the y-axis')
parser.set_defaults(no_y_axis=False)
parser.add_argument('--logy', dest='log_y', action='store_true', help='use a logarithmic y-axis')
parser.set_defaults(log_y=False)
parser.add_argument('--no-y-minor-ticks', dest='no_y_minor_ticks', action='store_true', help='force the logarithmic y-axis to include all minor ticks')
parser.set_defaults(no_y_minor_ticks=False)
parser.add_argument('--legend-only', dest='legend_only', action='store_true', help='use the data solely to produce a legend and render that legend')
parser.set_defaults(legend_only=False)
parser.add_argument('--legend-include', dest='legend_include', action='store_true', help='include a legend on the plot')
parser.set_defaults(legend_include=False)
parser.add_argument('--legend-columns', dest='legend_columns', type=int, default=1, help='number of columns to use to show legend entries')
parser.add_argument('--font-size', dest='font_size', type=int, default=20, help='font size to use in points (default: 20)')
parser.add_argument('--lightmode', dest='lightmode', action='store_true', help='enable light mode (disable dark mode)')
parser.set_defaults(lightmode=False)
parser.add_argument('--no-y-grid', dest='no_y_grid', action='store_true', help='remove all grids on y-axis')
parser.set_defaults(no_y_grid=False)
parser.add_argument('--no-y-minor-grid', dest='no_y_minor_grid', action='store_true', help='remove grid on y-axis minor ticks')
parser.set_defaults(no_y_minor_grid=False)
parser.add_argument('--error-bar-width', dest='error_bar_width', type=float, default=6, help='set width of error bars (default 6); 0 will disable error bars')
parser.add_argument('--stacked', dest='stacked', action='store_true', help='causes bars to be stacked')
parser.set_defaults(stacked=False)
parser.add_argument('--metadata', dest='metafile', type=argparse.FileType('r'), default=dict(), help='metadata json file to customize colors, titles, etc. sample is provided in metadata.json')
parser.add_argument('--ignore', dest='ignore', help='ignore the next argument')
parser.add_argument('--style-hooks', dest='style_hooks', default='', help='allows a filename to be provided that implements functions style_init(mpl), style_before_plotting(mpl, plot_kwargs_dict) and style_after_plotting(mpl). your file will be imported, and hooks will be added so that your functions will be called to style the mpl instance. note that plt/fig/ax can all be extracted from mpl.')


parser.add_argument('--y-min', dest='y_min', type=float, default=None, help='set the minimum value for the y-axis')
parser.add_argument('--y-max', dest='y_max', type=float, default=None, help='set the maximum value for the y-axis')

args = parser.parse_args()

meta = dict()
if args.metafile: 
    meta = json.load(args.metafile)
    
    
# set up matplotlib
mpl.use('Agg')
mpl.rc('hatch', color='k', linewidth=1)

if args.style_hooks != '':
    sys.path.append(os.path.dirname(args.style_hooks))
    module_filename = os.path.relpath(args.style_hooks, os.path.dirname(args.style_hooks)).replace('.py', '')
    import importlib
    mod_style_hooks = importlib.import_module(module_filename)
    mod_style_hooks.style_init(mpl)

else:
    rcParams.update({'figure.autolayout': True})
    rcParams.update({'font.size': args.font_size})
    if not args.lightmode:
        plt.style.use('dark_background')
    plt.rcParams["figure.dpi"] = args.dots_per_inch


# load data 
data = plot_tools.load_data(args.infile, meta)
if not len(data):
    print("ERROR: no data provided, so no graph to render.")
    quit()

tmean, tmin, tmax, tstd, tcount = plot_tools.calc_figures(data, meta)
print(tmean)
err = plot_tools.calc_error_bars(tmean, tmin, tmax, tstd, tcount)

if args.error_bar_width > 0 and len(err):
    for e in err[0]:
        if len([x for x in e.index]) <= 1:
            print("note : forcing NO error bars because index is too small: {}".format(e.index))
            args.error_bar_width = 0
elif not len(err):
    args.error_bar_width = 0
    
    
# make plot 
plot_kwargs = plot_tools.make_plot_kwargs(args, tmean, err)
legend_kwargs = plot_tools.make_legend_kwargs(args)
fig, ax = plt.subplots()

if args.style_hooks != '': 
    mod_style_hooks.style_before_plotting(mpl, plot_kwargs, legend_kwargs)

tmean.index = tmean.index.astype(str) # to even out the x-axis labels
chart = tmean.plot(fig=fig, ax=ax, **plot_kwargs)

chart.grid(axis='y', zorder=0)

ax = plt.gca()

# rotate xticks 90 degrees
if args.kind == 'bar':
    chart.set_xticklabels(chart.get_xticklabels(), ha="center", rotation=0)

plot_tools.handle_aesthetics(args, ax, meta, tmean, data)

plot_tools.set_common_plot_properties(args, ax)

if (args.legend_include): 
    plt.legend(**legend_kwargs)


if not args.legend_only: 
    if args.style_hooks != '': 
        mod_style_hooks.style_after_plotting(mpl)
    
    print(f"Saving figure {args.outfile.name}")
    plt.savefig(args.outfile.name)
    
if args.legend_only:
    handles, labels = ax.get_legend_handles_labels()
    fig_legend = plt.figure() #figsize=(12,1.2))
    axi = fig_legend.add_subplot(111)
    handles = [h[0] if isinstance(h, mpl.container.ErrorbarContainer) else h for h in handles]
    fig_legend.legend(handles, labels, loc='center', ncol=legend_kwargs['ncol'], frameon=False)
    # fig_legend.legend(handles, labels, loc='center', ncol=int(math.ceil(len(tpos_err)/2.)), frameon=False)
    axi.xaxis.set_visible(False)
    axi.yaxis.set_visible(False)
    axi.axes.set_visible(False)
    fig_legend.savefig(args.outfile.name, bbox_inches='tight')
