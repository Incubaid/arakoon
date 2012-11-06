import functools
import subprocess
import os
import string
import os.path

from optparse import OptionParser

OLD_CWD = os.getcwd()

parser = OptionParser()
parser.add_option("-r", "--root", dest="root", default="../ROOT",
                  help="Root directory for the env", metavar="ROOT")
(options, args) = parser.parse_args()

OCAML='4.00.1'
ROOT = os.path.realpath(options.root)
PREFIX = "%s/%s" % (ROOT,'OCAML')
OPAM_HOME='%s/OPAM' % ROOT
OPAM_ROOT='%s/OPAM_ROOT' % ROOT
INCUBAID_DEVEL='git://github.com/Incubaid/opam-repository-devel.git'



def sh(x, **kwargs):
    print string.join(x,' ')
    r =  subprocess.call(x,**kwargs)
    if r :
        print r
        raise RuntimeError("Failed to run %s %s" % (x, kwargs))

print PREFIX
env = {'PATH': string.join([PREFIX + '/bin',
                            OPAM_HOME + '/bin',
                            '/bin',
                            '/usr/bin',
                            ],':'),
       'HOME':os.environ['HOME']
       }

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

    def download(self, extra = None):
        fn = '%s/%s' % (ROOT, self._archive)
        start = ['wget']
        if extra:
            start.extend(extra)
        start.extend(['-O',fn,self._url])

        if not os.path.exists(fn):
            sh(start)

    def extract(self):
        flags = extract_flags[self._extension]
        sh(['tar', flags, self._archive], cwd = ROOT)

    def sh(self, cmd):
        d = '%s/%s' % (ROOT, self._name)
        sh(cmd, cwd = d, env = env)


def maybe_install_ocaml():
    if os.path.exists(PREFIX):
        print '%s exists, not installing ocaml compiler' % PREFIX
        return
    
    lib = Lib('ocaml-%s' % OCAML,'.tar.bz2',
              'http://caml.inria.fr/pub/distrib/ocaml-4.00/%s')
    lib.download()
    lib.extract()
    lib.sh(['./configure', '--prefix',PREFIX])
    lib.sh(['make','world.opt'])
    lib.sh(['make','install'])

def maybe_install_opam():
    if os.path.exists(OPAM_HOME):
        print '%s exists, not installing opam' % OPAM_HOME
        return
    
    lib = Lib('opam','',
              'https://github.com/OCamlPro/%s/tarball/master')
    lib.download()
    sh(['tar','-zxvf',lib._archive], cwd = ROOT)

    #which is the ocaml dir?
    files = os.listdir(ROOT)
    d = None
    for f in files:
        if f.startswith('OCamlPro'):
            d = ROOT + '/' + f
            break
    sh(['./configure', '--prefix=%s' % OPAM_HOME], cwd = d, env = env)
    sh(['make','clean'], cwd = d, env = env)
    sh(['make'], cwd = d, env = env)
    sh(['make','install'], cwd = d, env = env)

    

def maybe_install_packages():
    if os.path.exists(OPAM_ROOT):
        print '%s exists, not installing packages' % OPAM_ROOT
        return
    
    sh(['mkdir', '-p', OPAM_ROOT])
    opam_env = env.copy()
    def opam(x):
        sh(['opam','--verbose', '--yes', '--root', OPAM_ROOT] + x, env = opam_env)

    opam(['init'])
    opam(['remote', '-add', 'devel', '-kind', 'git', INCUBAID_DEVEL])
    opam(['update'])
    opam(['install', 'ocamlfind','baardskeerder'])
    
def do_it():
    sh(['mkdir', '-p', ROOT])
    maybe_install_ocaml()
    maybe_install_opam()
    maybe_install_packages()

    print 'now do the following to update your environment:'
    print 'eval `%s/bin/opam --root %s config -env`' % (OPAM_HOME, OPAM_ROOT)




if __name__ == '__main__':
    do_it()


