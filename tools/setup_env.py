import functools
import subprocess
import os
import string

from optparse import OptionParser

OLD_CWD = os.getcwd()

parser = OptionParser()
parser.add_option("-r", "--root", dest="root", default="../ROOT",
        help="Root directory for the env", metavar="ROOT")
parser.add_option("-y", "--no-x", dest="no_x", default=False,
        action="store_true", help="Do not build anything depending on X")
parser.add_option("-c", "--client", dest="client", default=False,
        action="store_true", help="Include the client interfaces")
(options, args) = parser.parse_args()

OCAML='3.12.1'
ROOT = os.path.realpath(options.root)
PREFIX = "%s/%s" % (ROOT,'OCAML')

def sh(x, **kwargs):
    print x
    if subprocess.call(x,**kwargs):
        raise RuntimeError("Failed to run %s %s" % (x, kwargs))

def mr_proper():
    sh (['rm','-rf', ROOT])


print PREFIX
env = {'PATH': string.join([PREFIX + '/bin',
                            '/bin',
                            '/usr/bin',
                            ],':')}

extract_flags = {
    '.tar.gz':'-zxvf',
    '.tar.bz2':'-jxvf',
    '.tbz': '-jxvf',
    }

class Lib:
    def __init__(self, name, extension, url_t):
        self._name = name
        self._extension = extension
        self._archive = name + extension
        self._url = url_t % self._archive

    def download(self):
        fn = '%s/%s' % (ROOT, self._archive)
        sh(['wget', '-O', fn, self._url])

    def extract(self):
        flags = extract_flags[self._extension]
        sh(['tar', flags, self._archive], cwd = ROOT)

    def sh(self, cmd):
        d = '%s/%s' % (ROOT, self._name)
        sh(cmd, cwd = d, env = env)

def install_ocaml():
    lib = Lib('ocaml-%s' % OCAML,'.tar.bz2',
              'http://caml.inria.fr/pub/distrib/ocaml-3.12/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure', '--prefix',PREFIX])
    lib.sh(['make','world.opt'])
    lib.sh(['make','install'])

def install_ocamlfind():
    lib = Lib('findlib-1.2.7', '.tar.gz',
              'http://download.camlcity.org/download/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure'])
    lib.sh(['make','all','opt','install'])

def install_ounit():
    lib = Lib('ounit-1.1.0','.tar.gz',
              'http://forge.ocamlcore.org/frs/download.php/495/%s')
    lib.download()
    lib.extract()
    lib.sh(['ocaml', 'setup.ml', '-configure'])
    lib.sh(['ocaml', 'setup.ml', '-build'])
    lib.sh(['ocaml', 'setup.ml', '-install'])

def install_react():
    lib = Lib('react-0.9.2','.tbz',
              'http://erratique.ch/software/react/releases/%s')
    lib.download()
    lib.extract()
    lib.sh(['chmod','u+x','build'])
    lib.sh(['./build'])
    lib.sh(['./build','install'])
    sh(['mv', '%s/lib/ocaml/react' % PREFIX, 
        '%s/lib/ocaml/site-lib/' % PREFIX])

def install_lwt():
    # Tell lwt where libev can be found
    env['LIBRARY_PATH'] = '%s/lib' % PREFIX
    env['C_INCLUDE_PATH'] = '%s/include' % PREFIX

    lib = Lib('lwt-2.3.1','.tar.gz',
              'http://ocsigen.org/download/%s')
    lib.download()
    lib.extract()
    lib.sh(['rm','-f',  'setup.data','setup.log'])
    lib.sh(['make','clean'])
    lib.sh(['ocaml', 'setup.ml', '-configure', '--prefix', PREFIX])
    lib.sh(['ocaml', 'setup.ml', '-build'])
    lib.sh(['ocaml', 'setup.ml', '-install'])

def install_camlbz2():
    lib = Lib('camlbz2-0.6.0','.tar.gz',
              'https://forge.ocamlcore.org/frs/download.php/72/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure'])
    lib.sh(['make', 'all'])
    lib.sh(['make', 'install'])

def install_lablgtk():
    lib = Lib('lablgtk-2.14.2',
              '.tar.gz',
              'http://wwwfun.kurims.kyoto-u.ac.jp/soft/olabl/dist/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure','--prefix=%s' % PREFIX])
    lib.sh(['make','world'])
    lib.sh(['make', 'install'])


def install_cairo_ocaml():
    lib = Lib('cairo-ocaml-1.2.0',
              '.tar.bz2',
              'http://cgit.freedesktop.org/cairo-ocaml/snapshot/%s')
    lib.download()
    lib.extract()
    lib.sh(['aclocal','-I','support'])
    lib.sh(['autoconf'])
    lib.sh(['./configure','--prefix=%s' % PREFIX])
    lib.sh(['make'])
    lib.sh(['make', 'install'])

def install_ocamlviz():
    lib = Lib('ocamlviz-1.01', '.tar.gz',
              'http://ocamlviz.forge.ocamlcore.org/%s')
    lib.download()
    lib.extract()
    # the rest is foefelare-trucare(TM)
    d = '%s/%s' % (ROOT, 'ocamlviz') ## differs from the rest
    sh(['autoconf'], cwd = d, env = env)
    sh(['./configure','--prefix=%s' % PREFIX ], cwd = d , env = env)
    sh(['make'], cwd = d, env = env)
    sh(['make','install'], cwd = d, env = env)
    LIBDIR = PREFIX + '/lib/ocaml/site-lib/ocamlviz'
    sh(['mkdir','-p',LIBDIR])
    files = ['src/ocamlviz.mli',
             'src/ocamlviz.cmi', 
             'src/ocamlviz_threads.cmi']
    cmd = ['cp','-f']
    cmd.extend(files)
    cmd.append(LIBDIR)
    sh(cmd, cwd = d)

    files2 = [
        'libocamlviz.a',
        'libocamlviz.cma',
        'libocamlviz.cmxa',
        'camlp4/pa_ocamlviz.cmi',
        'camlp4/pa_ocamlviz.cmo',
        ]
    cmd = ['cp','-f']
    cmd.extend(files2)
    cmd.append(LIBDIR)
    sh(cmd,cwd = d)
    sh(['cp','-f','ascii.opt', PREFIX + '/bin/ocamlviz-ascii'], cwd =d)
    sh(['cp','-f','gui.opt', PREFIX + '/bin/ocamlviz'], cwd = d)
    # create a META file
    meta = """
version = "1.0.1"
description = "real time profiling"
requires = "unix"
archive(byte) = "libocamlviz.cma"
archive(native) = "libocamlviz.cmxa"
exists_if = "libocamlviz.cma"

package "syntax" (
  exists_if = "pa_ocamlviz.cmo"
  description = "auto instrumentation sugars for ocamlviz"
  requires = "camlp4"
  archive(syntax,preprocessor) = "pa_ocamlviz.cmo"

)
"""
    f = open('%s/META' % LIBDIR, 'w')
    f.write(meta)
    f.close()

def install_libev():
    lib = Lib('libev-4.04','.tar.gz',
              'http://dist.schmorp.de/libev/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure', '--prefix=%s' % PREFIX])
    lib.sh(['make'])
    lib.sh(['make', 'install'])

def install_client():
    client_sh = functools.partial(sh, cwd=OLD_CWD, env=env)
    client_sh(["ocamlbuild", "-use-ocamlfind", "arakoon_client.cma", "arakoon_client.cmxa", "arakoon_client.a"])
    client_sh(["ocamlfind", "remove", "arakoon_client"])
    client_sh(["ocamlfind", "install", "arakoon_client",
        "META",
        "_build/src/arakoon_client.cma",
        "_build/src/arakoon_client.cmxa",
        "_build/src/client/arakoon_exc.mli",
        "_build/src/client/arakoon_exc.cmi",
        "_build/src/client/arakoon_client.mli",
        "_build/src/client/arakoon_client.cmi",
        "_build/src/client/arakoon_remote_client.mli",
        "_build/src/client/arakoon_remote_client.cmi",
        "_build/src/plugins/registry.mli",
        "_build/src/plugins/registry.cmi",
        "_build/src/node/node_cfg.cmi",
        "_build/src/tools/llio.mli",
        "_build/src/tools/llio.cmi",
        "_build/src/arakoon_client.a"
        ])

def do_it():
    mr_proper()
    sh(['mkdir', ROOT])
    install_ocaml()
    install_ocamlfind()
    install_ounit()
    install_react()
    install_libev()
    install_lwt()
    install_camlbz2()
    if not options.no_x:
        install_lablgtk()
        install_cairo_ocaml()
        install_ocamlviz()
    if options.client:
        install_client()
    #sudo cp lablgtk-2.14.2/examples/test.xpm /usr/share/pixmaps/ocaml.xpm
    print '\n\nnow prepend %s/bin to your PATH' % PREFIX

if __name__ == '__main__':
    do_it()
