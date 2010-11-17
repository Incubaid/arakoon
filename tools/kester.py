import os

header = """
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
""".strip()

def maybe_add_header(fn,pre,post):
    data = None
    with open(fn,'r') as f:
        data = f.read()
    actual = pre + header + post
    if data.startswith(actual) :
        print "skipping %s : already done" % fn
    else:
        new_data = actual + data
        with open(fn,'w') as f:
            f.write(new_data)

def process_dir(directory):
    for dirpath, dirnames, filenames in os.walk(directory):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            if fn.endswith('.py'):
                print full, "==> python"
                maybe_add_header(full,"""'''\n""","""\n'''\n\n""")
            elif fn.endswith('.ml'):
                print full, "==> ocaml"
                maybe_add_header(full,"""(*\n""","""\n*)\n\n""")
            
if __name__ == "__main__":
	process_dir('../src/')
	process_dir('../extension/')