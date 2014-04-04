"""
This script generates a confluence wiki page for one or more runs of the
Arakoon benchmark client. It generates plots for the numeric data it scrapes
from the log files.

The information about the ran benchmarks is extracted from the filenames of the
logs. If logs are passed for different benchmark settings, this is detected,
and the benchmark settings will be diffed and *only* the differences will be
used to identify the different benchmarks.
"""

import collections
import itertools
import os
import re
import sys

import numpy
import matplotlib
import matplotlib.pyplot

import times_to_csv

def sorted_iteritems(d):
    sorted_keys = sorted(d.keys())
    for key in sorted_keys:
        yield key, d[key]

def get_param(param_name, s, tc_params, default=None):
    regex = "%s(-?\d+)" % param_name
    m = re.search(regex, s)
    if m:
        tc_params[param_name] = int(m.groups()[0])
    elif default is not None:
        tc_params[param_name] = default

def extract_params(path, default_loglevel="unknown"):
    filename = os.path.basename(path)

    relevant = filename.replace("output_for_value_", "").replace(".txt", "")

    value_size_match = re.match("^(\d+)", relevant)
    if not value_size_match:
        raise ValueError("Failed to parse file name %s" % filename)

    value_size = int(value_size_match.groups()[0])

    aratype = "native" if "native" in relevant else "bytecode"
    if "info" in relevant:
        loglevel = "info"
    elif "debug" in relevant:
        loglevel = "debug"
    else:
        loglevel = default_loglevel

    if "2disk" in relevant:
        n_disks = 2
    elif "3disk" in relevant:
        n_disks = 3
    else:
        n_disks = 1

    if "ssd" in relevant:
        db_disk_type = "ssd"
    else:
        db_disk_type = "hdd"

    if "3node" in relevant:
        n_nodes = 3
    else:
        n_nodes = 1

    if "1bm" in relevant:
        n_bm = 1
    elif "3bm" in relevant:
        n_bm = 3
    else:
        n_bm = 1

    if "ext2" in relevant:
        db_fs = "ext2"
    else:
        db_fs = "ext4"

    tc_params = {}
    get_param("bnum", relevant, tc_params)
    get_param("apow", relevant, tc_params)
    get_param("lmemb", relevant, tc_params)
    get_param("nmemb", relevant, tc_params)
    get_param("fpow", relevant, tc_params, 10)

    bench_params = {}
    get_param("tx", relevant, bench_params, default=100)
    get_param("max_n", relevant, bench_params, default=1000*1000)
    get_param("node_at", relevant, bench_params)

    return value_size, {
            "aratype": aratype,
            "loglevel": loglevel,
            "n_disks": n_disks,
            "db_disk_type": db_disk_type,
            "tc": tc_params,
            "bench": bench_params,
            "n_bm": n_bm,
            "n_nodes": n_nodes,
            "db_fs": db_fs,
            }

def get_line_style_iter():
    return itertools.cycle(['-', '--', '-.', ':'])

def get_color_iter():
    return itertools.cycle(['b', 'g', 'r', 'c', 'm', 'y', 'k'])

class Plotter(object):
    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.data = collections.defaultdict(dict)

    def register(self, group, value_size, set_times, get_times, bench_times):
        self.data[group][value_size] = (set_times, get_times, bench_times)

    def generate(self):
        #self.generate_sets()
        self.generate_sets_per_second()
        #self.generate_gets()
        self.generate_gets_per_second()
        self.generate_tx_plots()

    def generate_tx_plots(self):
        tx_set_times_dict = collections.defaultdict(list)
        tx_get_times_dict = collections.defaultdict(list)
        all_values = sum([value_dict.keys() for value_dict in self.data.itervalues()], [])
        value_sizes = sorted(set(all_values))

        for group, value_size_dict in sorted_iteritems(self.data):
            for value_size in value_sizes:
                times_tuple = value_size_dict.get(value_size, None)
                if times_tuple:
                    _, _, bench_times = times_tuple
                    _, total_tx_set_time, _, total_tx_get_time = bench_times
                else:
                    total_tx_set_time, total_tx_get_time = 0, 0

                tx_set_times_dict[group].append(total_tx_set_time)
                tx_get_times_dict[group].append(total_tx_get_time)

        self._generate_tx_plot("Times for sets in a transaction (less is better)", value_sizes, tx_set_times_dict, "tx_set_times.png")
        self._generate_tx_plot("Times for gets in a transaction (less is better)", value_sizes, tx_get_times_dict, "tx_get_times.png")

    def _generate_tx_plot(self, title, value_sizes, times_dict, file_name):
        png_path = os.path.join(self.output_folder, file_name)
        xlabel = "Value size in bytes"
        ylabel = "Amount of time in seconds"
        n_bars_per_size = len(times_dict)
        padding = 0.2
        width = (1 - padding) / n_bars_per_size
        color_iter = get_color_iter()
        x_numbers = numpy.arange(len(value_sizes)) + 1

        fig = matplotlib.pyplot.figure(figsize=(7, 5))
        ax = fig.add_subplot(111)
        for n, (label, times) in enumerate(sorted_iteritems(times_dict)):
            offset = (-n_bars_per_size / 2) + n
            ax.bar(x_numbers + (offset * width), times, width, label=label, color=color_iter.next())

        matplotlib.pyplot.title(title)
        matplotlib.pyplot.xticks(x_numbers, [str(vs) for vs in value_sizes])
        matplotlib.pyplot.xlabel(xlabel)
        matplotlib.pyplot.ylabel(ylabel)
        matplotlib.pyplot.legend(loc='upper center')
        matplotlib.pyplot.savefig(png_path)

    def generate_sets(self):
        matplotlib.pyplot.clf()
        png_path = os.path.join(self.output_folder, "sets.png")
        for label in sorted(self.data.keys()):
            set_times, get_times, _ = self.data[label]
            amounts = [tt[0] for tt in set_times]
            times = [tt[1] for tt in set_times]
            matplotlib.pyplot.plot(amounts, times, label=str(label))
        matplotlib.pyplot.legend()
        matplotlib.pyplot.savefig(png_path)

    def generate_gets(self):
        matplotlib.pyplot.clf()
        png_path = os.path.join(self.output_folder, "gets.png")
        for label in sorted(self.data.keys()):
            set_times, get_times, _ = self.data[label]
            amounts = [tt[0] for tt in get_times]
            times = [tt[1] for tt in get_times]
            matplotlib.pyplot.plot(amounts, times, label=str(label))
        matplotlib.pyplot.legend()
        matplotlib.pyplot.savefig(png_path)

    def generate_sets_per_second(self):
        self.sets_per_second = self._generate_per_second_plot("sets_per_second.png", times_index=0, title="Sets per second (more is better)", xlabel="Amount of sets done", ylabel="Amount of sets per second")

    def generate_gets_per_second(self):
        self.gets_per_second = self._generate_per_second_plot("gets_per_second.png", times_index=1, title="Gets per second (more is better)", xlabel="Amount of gets done", ylabel="Amount of gets per second")

    def _generate_per_second_plot(self, filename, times_index, title, xlabel, ylabel):
        if len(self.data) > 2:
            return self._generate_per_second_plot_per_value_size(filename, times_index, title, xlabel, ylabel)
        else:
            return self._generate_per_second_plot_for_all_values(filename, times_index, title, xlabel, ylabel)

    def _gen(self, data, title, xlabel, ylabel, path):
        matplotlib.pyplot.clf()
        matplotlib.pyplot.figure(figsize=(12, 6))
        matplotlib.pyplot.title(title)
        matplotlib.pyplot.subplots_adjust(right=0.5)
        for xdata, ydata, line_style, label in data:
            matplotlib.pyplot.plot(xdata, ydata, line_style, label=label)
        matplotlib.pyplot.xlabel(xlabel)
        matplotlib.pyplot.ylabel(ylabel)
        matplotlib.pyplot.legend(loc="center right", bbox_to_anchor=(2.0, 0.5))
        matplotlib.pyplot.savefig(path)

    def _generate_per_second_plot_per_value_size(self, filename, times_index, title, xlabel, ylabel):
        png_paths = []
        data = collections.defaultdict(list)
        line_style_iter = get_line_style_iter()
        for group, value_dict in sorted_iteritems(self.data):
            line_style = line_style_iter.next()
            for value_size, times_list in sorted_iteritems(value_dict):
                times = times_list[times_index]
                prev_amount = 0
                prev_time = 0
                sets_per_second = []
                for amount, time in times:
                    sets_per_second.append((amount - prev_amount) * 1.0 / (time - prev_time))

                amounts = [tt[0] for tt in times]
                label = group
                data[value_size].append((amounts, sets_per_second, line_style, label))
        for value_size, value_size_data in sorted_iteritems(data):
            png_path = os.path.join(self.output_folder, "size_%d_%s" % (value_size, filename))
            value_size_title = "%s - Value Size %d" % (title, value_size)
            self._gen(value_size_data, value_size_title, xlabel, ylabel, png_path)
            png_paths.append(png_path)
        return png_paths

    def _generate_per_second_plot_for_all_values(self, filename, times_index, title, xlabel, ylabel):
        data = []
        png_path = os.path.join(self.output_folder, filename)
        line_style_iter = get_line_style_iter()
        for group, value_dict in sorted_iteritems(self.data):
            line_style = line_style_iter.next()
            for value_size, times_list in sorted_iteritems(value_dict):
                times = times_list[times_index]
                prev_amount = 0
                prev_time = 0
                sets_per_second = []
                for amount, time in times:
                    sets_per_second.append((amount - prev_amount) * 1.0 / (time - prev_time))

                amounts = [tt[0] for tt in times]
                if group:
                    label = "%s - %s" % (group, value_size)
                else:
                    label = str(value_size)
                data.append((amounts, sets_per_second, line_style, label))
        self._gen(data, title, xlabel, ylabel, png_path)
        return [png_path]

def name_for_params(params):
    name = "arabench_%(aratype)s_%(loglevel)s_%(n_disks)d%(db_disk_type)s" % params

    tc_details = []
    tc_params = params["tc"]
    for key in sorted(tc_params.keys()):
        tc_details.append("%s%02d" % (key, tc_params[key]))

    bench_details = []
    bench_params = params["bench"]
    for key in sorted(bench_params.keys()):
        bench_details.append("%s%s" % (key, bench_params[key]))

    return "_".join([x for x in (name, "_".join(tc_details), "_".join(bench_details)) if x])

def folder_for_params(params):
    name = "arabench_%(aratype)s_%(loglevel)s_%(n_disks)d%(db_disk_type)s" % params

    tc_details = []
    tc_params = params["tc"]
    for key in sorted(tc_params.keys()):
        tc_details.append("%s%02d" % (key, tc_params[key]))

    bench_details = []
    bench_params = params["bench"]
    for key in sorted(bench_params.keys()):
        bench_details.append("%s%s" % (key, bench_params[key]))

    return "_".join([x for x in (name, "_".join(tc_details), "_".join(bench_details)) if x])

def group_for_params(params):
    parts = []
    for key, value in sorted_iteritems(params):
        if key == "tc":
            s = group_for_params(value)
            if s:
                parts += ["tc_" + s]
        elif key == "bench":
            s = group_for_params(value)
            if s:
                parts += ["bench_" + s]
        else:
            if isinstance(value, int):
                parts.append("%s%02d" % (key, value))
            else:
                parts.append("%s%s" % (key, value))

    return "_".join(parts)

def folder_for_diff_params(params):
    parts = []
    for key, values in sorted_iteritems(params):
        if key == "tc":
            s = folder_for_diff_params(values)
            if s:
                parts += ["tc_" + s]
        elif key == "bench":
            s = folder_for_diff_params(values)
            if s:
                parts += ["bench_" + s]
        else:
            values_string = "_vs_".join(str(v) for v in values)
            parts.append("%s_%s" % (key, values_string))

    return "_".join(parts)

def parse_times(log_file_name):
    data = open(log_file_name).read()

    set_time_match = re.search("fill \d+ took: (?P<total_set_time>\d+\.\d+)", data)
    set_time_data_dict = set_time_match.groupdict()
    set_time = set_time_data_dict["total_set_time"]

    set_time_tx_match = re.search("fill_transactions \d+ took : (?P<total_set_time_tx>\d+\.\d+)", data)
    set_time_tx_data_dict = set_time_tx_match.groupdict()
    set_time_tx = set_time_tx_data_dict["total_set_time_tx"]

    get_time_match = re.search("get of \d+ values \(random keys\) took: (?P<total_get_time>\d+\.\d+)", data)
    get_time_data_dict = get_time_match.groupdict()
    get_time = get_time_data_dict["total_get_time"]

    get_time_tx_match = re.search("multiget of \d+ values \(random keys\) in transactions of size \d+ took (?P<total_get_time_tx>\d+\.\d+)", data)
    get_time_tx_data_dict = get_time_tx_match.groupdict()
    get_time_tx = get_time_tx_data_dict["total_get_time_tx"]


    return [int(float(n)) for n in (set_time, set_time_tx, get_time, get_time_tx)]

def generate_wiki_content(title, info_dict, sets_per_second, gets_per_second):
    title = "Benchmark: %s" % title
    setups = {}

    for log_file_name, info in sorted_iteritems(info_dict):
        group = group_for_params(info["params_diff"])
        params = info["params"]
        setup = []

        if params["aratype"] == "native":
            setup.append("*  Native arakoon")
        elif params["aratype"] == "bytecode":
            setup.append("*  Bytecode arakoon")
        else:
            raise ValueError("Unknown aratype %s" % params["aratype"])

        setup.append("*  Loglevel %s" % params["loglevel"])

        if params["n_disks"] == 1:
            setup.append("*  Arakoon logs and db on the same disk")
        elif params["n_disks"] == 2:
            setup.append("*  Dedicated db and tlog disk")
        elif params["n_disks"] == 3:
            setup.append("*  Dedicated db disk")
        else:
            ValueError("Can't handle %d disks" % params["n_disks"])

        if params["db_disk_type"] == "hdd":
            setup.append("*  Arakoon db on a spinning disk drive")
        elif params["db_disk_type"] == "ssd":
            setup.append("*  Arakoon db on an SSD drive")
        else:
            ValueError("Unknown disk type %s" % params["db_disk_type"])

        setup.append("*  DB fs is %s" % params["db_fs"])
        setup.append("*  %d Arakoon node(s)" % params["n_nodes"])
        setup.append("*  %d benchmark client(s)" % params["n_bm"])

        setup.append("*  Tokyo Cabinet setup:")
        tc_params = params["tc"]
        for name in ("Bnum", "Apow", "Lmemb", "Nmemb", "Fpow"):
            key = name.lower()
            if key in tc_params:
                setup.append("**  %s: %d" % (name, tc_params[key]))

        setup.append("*  Benchmark setup:")
        bench_params = params["bench"]
        setup.append("""**  Size of the transactions: %(tx)d
**  Amount of individual gets and sets: %(max_n)d
""" % bench_params)

        setups[group] = setup

    times = collections.defaultdict(dict)

    for log_file_name, info in sorted_iteritems(info_dict):
        value_size = info["value_size"]
        group = group_for_params(info["params_diff"])
        bench_times = parse_times(log_file_name)
        times[value_size][group] = bench_times

    times_table_lines = ["|| Value Size (bytes) || Benchmark ID || Fill time (s) || Transaction fill time (s) || Get time (s) || Transaction get time (s) ||"]
    for value_size, value_size_times in sorted_iteritems(times):
        first = True
        for group, bench_times in sorted_iteritems(value_size_times):
            times_table_lines.append("| %s | %s | %s | %s | %s | %s |" % tuple([value_size if first else "", group] + bench_times))
            first = False

    if len(setups) == 1:
        setup_details = [
                "\n".join(setup) for setup in setups.itervalues()
                ]
    else:
        setup_details = [
                "h3. Benchmark ID: %s\n%s" % (group, "\n".join(setup)) for group, setup in setups.iteritems()
                ]
    context = {
            "title": title,
            "setup_details": "\n".join(setup_details),
            "time_table": "\n".join(times_table_lines),
            "sets_per_second": "\n".join(["!%s!" % os.path.basename(path) for path in sets_per_second]),
            "gets_per_second": "\n".join(["!%s!" % os.path.basename(path) for path in gets_per_second]),
            }
    return """
Parent: [BM]

h1. %(title)s
h2. Setup Details
%(setup_details)s

h2. Time Table
The value sizes are bytes. The times are seconds.

%(time_table)s

h2. Plots
h3. Transaction Sets per Second
!tx_set_times.png!

h3. Transaction Gets per Second
!tx_get_times.png!

h3. Sets per second
%(sets_per_second)s

h3. Gets per second
%(gets_per_second)s
    """ % context

def extract_key(value_size, params):
    return folder_for_params(params)

def get_all_params(log_file_paths):
    all_params = []
    for log_file_path in log_file_paths:
        value_size, params = extract_params(log_file_path)
        all_params.append(params)
    return all_params

def fill_params_diff(info_dict):
    diff_dict = collections.defaultdict(set)
    diff_dict["tc"] = collections.defaultdict(set)
    diff_dict["bench"] = collections.defaultdict(set)
    for log_file_path, info in info_dict.iteritems():
        for key, value in info["params"].iteritems():
            if key == "tc":
                for tc_key, tc_value in value.iteritems():
                    diff_dict["tc"][tc_key].add(tc_value)
            elif key == "bench":
                for bench_key, bench_value in value.iteritems():
                    diff_dict["bench"][bench_key].add(bench_value)
                diff_dict["bench"]
            else:
                diff_dict[key].add(value)

    for key, values in diff_dict.iteritems():
        if key == "tc":
            for tc_key, tc_values in values.iteritems():
                if len(tc_values) == 1:
                    continue

                for info in info_dict.itervalues():
                    info["params_diff"][key][tc_key] = info["params"][key][tc_key]
        elif key == "bench":
            for bench_key, bench_values in values.iteritems():
                if len(bench_values) == 1:
                    continue

                for info in info_dict.itervalues():
                    info["params_diff"][key][bench_key] = info["params"][key][bench_key]
        else:
            if len(values) == 1:
                continue

            for info in info_dict.itervalues():
                info["params_diff"][key] = info["params"][key]

def build_info_dict(log_file_paths):
    log_info = {}
    for log_file_path in log_file_paths:
        value_size, params = extract_params(log_file_path)
        log_info[log_file_path] = {
                "value_size": value_size,
                "params": params,
                "params_diff": {"tc": {}, "bench": {}},
                }

    fill_params_diff(log_info)
    return log_info

def diff_params(all_params):
    # Assumes all params dicts have the same keys:
    diff = {}
    for key in all_params[0].iterkeys():
        values = [params[key] for params in all_params]
        if key == "bench":
            subdiff = diff_params(values)
            diff[key] = subdiff
        elif key == "tc":
            subdiff = diff_params(values)
            diff[key] = subdiff
        else:
            values = set(values)
            if len(values) > 1:
                diff[key] = values
    return diff

def decide_output_folder(log_file_paths):
    all_params = get_all_params(log_file_paths)

    param_diff = diff_params(all_params)
    if param_diff == {"tc": {}, "bench": {}}:
        return folder_for_params(all_params[0])
    else:
        return folder_for_diff_params(param_diff)

def main():
    import getpass
    import json
    import xmlrpclib
    title = sys.argv[1]
    config_path = sys.argv[2]
    with open(config_path) as config_fp:
        config = json.load(config_fp)

    log_file_paths = config[title]
    if not log_file_paths:
        print "Please provide at least one log file"
        return

    info_dict = build_info_dict(log_file_paths)
    output_folder = decide_output_folder(log_file_paths)
    print output_folder
    plotter = Plotter(output_folder)

    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    for log_file_path, info in info_dict.iteritems():
        value_size = info["value_size"]
        params_diff = info["params_diff"]
        group = group_for_params(params_diff)
        with open(log_file_path) as log_file:
            log_file_lines = log_file.readlines()
            if log_file_lines:
                set_times = times_to_csv.parse_set_times(log_file_lines)
                get_times = times_to_csv.parse_get_times(log_file_lines)
                bench_times = parse_times(log_file_path)

                plotter.register(group, value_size, set_times, get_times, bench_times)

    plotter.generate()
    wiki_content = generate_wiki_content(title, info_dict, plotter.sets_per_second, plotter.gets_per_second)
    username = config["username"]
    password = getpass.getpass("Please provide the password for confluence user %s: " % username)

    server = xmlrpclib.ServerProxy('http://confluence.incubaid.com/rpc/xmlrpc');
    token = server.confluence1.login(username, password)
    space = "ARAKOON"
    page_info = server.confluence1.getPage(token, space, title)
    page_info['content'] = wiki_content
    server.confluence1.storePage(token, page_info)

    print "Uploading attachments..."
    attachments = plotter.sets_per_second + plotter.gets_per_second + [os.path.join(output_folder, filename) for filename in ("tx_set_times.png", "tx_get_times.png")]
    for attachment_path in attachments:
        with open(attachment_path) as attachment_fp:
            attachment_data = attachment_fp.read()
        attachment_info = {
                "fileName": os.path.basename(attachment_path),
                "contentType": "image/png",
                }

        server.confluence1.addAttachment(token, page_info['id'], attachment_info, xmlrpclib.Binary(attachment_data))

if __name__ == "__main__":
    main()
