# This file is part of Arakoon, a distributed key-value store. Copyright
# (C) 2010 Incubaid BVBA

# Licensees holding a valid Incubaid license may use this file in
# accordance with Incubaid's Arakoon commercial license agreement. For
# more information on how to enter into this agreement, please contact
# Incubaid (contact details can be found on www.arakoon.org/licensing).

# Alternatively, this file may be redistributed and/or modified under
# the terms of the GNU Affero General Public License version 3, as
# published by the Free Software Foundation. Under this license, this
# file is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.

# See the GNU Affero General Public License for more details.
# You should have received a copy of the
# GNU Affero General Public License along with this program (file "COPYING").
# If not, see <http://www.gnu.org/licenses/>.

import csv
import os
import re
import sys

class TimesWriter(object):
    def __init__(self):
        self.times = {}

    def register(self, name, amount_time_pairs):
        if name in self.times:
            raise ValueError("Non-unique name")

        self.times[name] = dict(amount_time_pairs)

    def write(self, csv_file):
        key_sets = [set(times_dict.keys()) for times_dict in self.times.itervalues()]
        amounts_set = set().union(*key_sets)
        amounts = sorted(amounts_set)
        csv_file.writerow(["Amounts:"] + amounts)
        for name in sorted(self.times.keys()):
            times = self.get_times(name, amounts)
            csv_file.writerow([name] + times)

    def get_times(self, name, amounts):
        return [self.times[name].get(amount, "") for amount in amounts]

def get_fill_lineno(log_file_lines):
    for lineno, line in enumerate(log_file_lines):
        if line.startswith("fill "):
            return lineno

    raise ValueError("Not a valid log file")

def get_gets_start_lineno(log_file_lines):
    for lineno, line in enumerate(log_file_lines):
        if "random keys" in line:
            return lineno + 1

    raise ValueError("Not a valid log file")

def get_gets_end_lineno(log_file_lines):
    for lineno, line in enumerate(log_file_lines):
        if line.startswith("get of "):
            return lineno

    raise ValueError("Not a valid log file")

def parse_amount_time_pairs(txt):
    amount_time_pairs = re.findall("(?P<amount>\d+)\s+\(\s+(?P<time>\d+)s\)", txt)
    return [(int(amount), int(time)) for (amount, time) in amount_time_pairs]

def parse_set_times(log_file_lines):
    fill_lineno = get_fill_lineno(log_file_lines)
    fill_txt = " ".join(line for line in log_file_lines[:fill_lineno])
    return parse_amount_time_pairs(fill_txt)

def parse_get_times(log_file_lines):
    get_start_lineno = get_gets_start_lineno(log_file_lines)
    get_end_lineno = get_gets_end_lineno(log_file_lines)
    get_txt = " ".join(line for line in log_file_lines[get_start_lineno:get_end_lineno] if line.startswith(" "))
    return parse_amount_time_pairs(get_txt)

def name_from_path(path):
    basename = os.path.basename(path)
    cleaned_basename = basename.replace("output_for_value_", "").replace(".txt", "")
    if "_" in cleaned_basename:
        nr, rest = cleaned_basename.split("_", 1)
        return "%s %s" % (rest, nr)
    else:
        return cleaned_basename

if __name__ == "__main__":
    csv_path = sys.argv[1]
    log_file_paths = sys.argv[2:]
    parsed_logs = []

    get_times_writer = TimesWriter()
    set_times_writer = TimesWriter()

    for log_file_path in log_file_paths:
        with open(log_file_path) as log_file:
            name = name_from_path(log_file_path)
            log_file_lines = log_file.readlines()
            if log_file_lines:
                try:
                    set_times = parse_set_times(log_file_lines)
                    get_times = parse_get_times(log_file_lines)
                    set_times_writer.register(name, set_times)
                    get_times_writer.register(name, get_times)
                except ValueError:
                    print "Skipping file %s" % log_file_path

    csv_file = csv.writer(open(csv_path, 'w'))
    set_times_writer.write(csv_file)
    get_times_writer.write(csv_file)
