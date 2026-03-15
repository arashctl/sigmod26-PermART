#!/usr/bin/python3

import numpy as np
import platform
import sys, getopt
import fileinput
import argparse

######################
## parse arguments
######################

parser = argparse.ArgumentParser(description='Produce a single-curve (x,y)-line plot from TWO COLUMN data (or ONE COLUMN y-data) provided via a file or stdin.')
parser.add_argument('-i', dest='infile', type=argparse.FileType('r'), default=sys.stdin, help='input file containing lines of form <x> <y> (or lines of form <y>); if none specified then will use stdin')
parser.add_argument('-o', dest='outfile', type=argparse.FileType('w'), default='out.png', help='output file with any image format extension such as .png or .svg; if none specified then plt.show() will be used')
parser.add_argument('-t', dest='title', default="", help='title string for the plot')
parser.add_argument('--suptitle', dest='suptitle', default="", help='super-title string for the plot (above the title)')
parser.add_argument('--title-total', dest='title_total', action='store_true', help='add the total of all y-values to the title; if the title contains {} it will be replaced by the total; otherwise, the total will be appended to the end of the string')
parser.set_defaults(title_total=False)
parser.add_argument('--x-title', dest='x_title', default="", help='title for the x-axis')
parser.add_argument('--y-title', dest='y_title', default="", help='title for the y-axis')
parser.add_argument('--no-y-min', dest='no_ymin', action='store_true', help='eliminate the minimum y-axis value of 0')
parser.set_defaults(no_ymin=False)
parser.add_argument('--lightmode', dest='lightmode', action='store_true', help='enable light mode (disable dark mode)')
parser.set_defaults(lightmode=False)
parser.add_argument('--trim-prefix-zeros', dest='trim_prefix_zeros', action='store_true', help='remove the largest possible prefix of the input data (if any) that consists of entirely zeros')
parser.set_defaults(trim_prefix_zeros=False)
parser.add_argument('--scalefactor', dest='scalefactor', type=int, default=1, help='optional constant to multiple all data points by (for scaling data)')
parser.add_argument('--fontsize', dest='fontsize', type=int, default=16, help='optional fontsize in points for all text')
parser.add_argument('--heightinches', dest='heightinches', type=float, default=5, help='optional height in inches for the plot')
parser.add_argument('--widthinches', dest='widthinches', type=float, default=12, help='optional width in inches for the plot')
args = parser.parse_args()

# parser.print_usage()
if len(sys.argv) < 2:
    if sys.stdin.isatty():
        parser.print_usage()
        print('waiting on stdin for data...')

# print('args={}'.format(args))

WIN = (platform.system() == 'Windows' or 'CYGWIN' in platform.system())

######################
## load data
######################

x = []
y = []

one_col = False
two_col = False

nonzero_seen = False
i=0
# print(args.infile)
for line in args.infile:
    i=i+1
    line = line.rstrip('\r\n')
    if (line == '' or line == None):
        continue

    tokens = line.split(" ")
    if len(tokens) == 2:
        two_col = True

        if not tokens[1] or tokens[1] == '' or tokens[1] == 'null': continue ## skip lines with an empty token...

        ## if trim_prefix_zero mode is enabled, and the y-value is a zero, and we haven't seen any non-zeros yet, we're trimming, so skip...
        if args.trim_prefix_zeros and not nonzero_seen and int(tokens[1]) == 0: continue
        if not nonzero_seen and int(tokens[1]) != 0: nonzero_seen = True

        ## if the first col is text, let's assume it's really single column y-data
        try:
            x.append(int(tokens[0]))
        except:
            x.append(i)

        y.append(args.scalefactor * int(tokens[1]))

    elif len(tokens) == 1:
        one_col = True

        if not tokens[0] or tokens[0] == '' or tokens[0] == 'null': continue ## skip line consisting of an empty token...

        ## if trim_prefix_zero mode is enabled, and the y-value is a zero, and we haven't seen any non-zeros yet, we're trimming, so skip...
        if args.trim_prefix_zeros and not nonzero_seen and int(tokens[0]) == 0: continue
        if not nonzero_seen and int(tokens[0]) != 0: nonzero_seen = True

        x.append(int(i))
        y.append(args.scalefactor * int(tokens[0]))

    else:
        print("ERROR at line {}: '{}'".format(i, line))
        exit(1)

if not len(x):
    print("ERROR: no data provided, so no graph to render.")
    quit()

if one_col and two_col:
    print("ERROR: cannot supply both one-column and two-column data lines.")
    quit()

######################
## setup matplotlib
######################

import matplotlib as mpl
if WIN:
    mpl.use('TkAgg')
else:
    mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
if not args.lightmode:
    plt.style.use('dark_background')

plt.rc('font', size=args.fontsize)

######################
## setup plot
######################

if args.title_total:
    import locale
    locale.setlocale(locale.LC_ALL, '')
    sumstr = '{:n}'.format(sum(y))
    if '{}' in args.title:
        _title = args.title.format(sumstr)
    else:
        _title = '{}{}'.format(args.title, sumstr)
    # _title = "{}{}".format(args.title, sum(y))
elif args.title != '':
    _title = args.title
else:
    _title = ''

dots_per_inch = 200
height_inches = args.heightinches
width_inches = args.widthinches

fig = plt.figure(figsize=(width_inches, height_inches), dpi=dots_per_inch)
ax = plt.gca()

if _title: plt.title(_title, fontsize=args.fontsize)

## set plot suptitle if applicable
if args.suptitle:
    fig.suptitle(args.suptitle, fontsize=args.fontsize)


#print('data len={}'.format(len(x)))
plt.plot(x, y, color='red')

if not args.no_ymin: plt.ylim(bottom=0)



## set x axis title

if args.x_title == "" or args.x_title == None:
    ax.xaxis.label.set_visible(False)
else:
    ax.xaxis.label.set_visible(True)
    ax.set_xlabel(args.x_title, fontsize=args.fontsize)

## set y axis title

if args.y_title == "" or args.y_title == None:
    ax.yaxis.label.set_visible(False)
else:
    ax.yaxis.label.set_visible(True)
    ax.set_ylabel(args.y_title, fontsize=args.fontsize)



######################
## y axis major, minor
######################


ax.grid(which='major', axis='y', linestyle='-', color='lightgray')

ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
ax.grid(which='minor', axis='y', linestyle='dotted', color='gray')

######################
## x axis major, minor
######################

ax.grid(which='major', axis='x', linestyle='-', color='lightgray')

ax.xaxis.set_minor_locator(ticker.AutoMinorLocator())
ax.grid(which='minor', axis='x', linestyle='dotted', color='gray')

######################
## do the plot
######################

plt.tight_layout()

if args.outfile == None:
    if WIN:
        mng = plt.get_current_fig_manager()
        mng.window.state('zoomed')
    plt.show()
else:
    print("saving figure %s" % args.outfile.name)
    plt.savefig(args.outfile.name)
