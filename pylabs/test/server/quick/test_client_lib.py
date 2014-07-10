"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from Compat import X
import os

def test_client_lib():
    my_temp = '/tmp/client_lib_test'
    lib_install = '/tmp/client_lib_install'
    os.environ['DESTDIR'] = lib_install
    cmds = [
        (['pwd'], None),
        (['ls', '-ahl'], None),
#        (['make', 'uninstall_client'], None),
#        (['make', 'clean'], None),
#        (['make'], None),
        (['make', 'install_client'], None),
        (['mkdir', '-p',  my_temp], None),
        (['mkdir', '-p',  lib_install + "/usr/lib/ocaml/arakoon_client"], None),
        (['cp', './examples/ocaml/demo.ml', './examples/ocaml/_tags', my_temp], None),
        (['ocamlbuild', '-use-ocamlfind', 'demo.native'], my_temp),
        (['make', 'uninstall_client'], None),
    ]
    for cmd, cwd in cmds:
        if cwd == None:
            cwd = '../../../workspace'
        r = X.subprocess.check_output(cmd, cwd = cwd)
        print r
