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
import logging
import subprocess

def test_client_lib():
    my_temp = '/tmp/client_lib_test'
    OCAML_LIBDIR = X.subprocess.check_output('ocamlfind printconf destdir',
                                             shell=True)
    OCAML_LIBDIR = OCAML_LIBDIR.strip()
    env = os.environ.copy()
    env['OCAML_LIBDIR'] = OCAML_LIBDIR
    cmds = [
        (['make', 'uninstall_client'], None),
        (['make', 'install'], None),
        (['mkdir', '-p',  my_temp], None),
        (['cp', './examples/ocaml/demo.ml', my_temp], None),
        (['ocamlbuild', '-use-ocamlfind', '-package','lwt' ,
          '-package','arakoon_client',
          '-tags', 'annot,debug,thread',
          'demo.native'], my_temp),
        (['make', 'uninstall_client'], None),
    ]
    for cmd, cwd in cmds:
        if cwd == None:
            cwd = '../..'
        print cmd
        try:
            r = X.subprocess.check_output(cmd,
                                          cwd = cwd,
                                          env = env,
                                          stderr= X.subprocess.STDOUT
            )
            print r
        except subprocess.CalledProcessError as ex:
            logging.info("ex:%s" % ex)
            logging.info("output=%s" % ex.output)
            raise ex
