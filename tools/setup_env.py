import subprocess
import os
import string

OCAML='3.12.1'
ROOT = '../ROOT' #keep it outside our own tree otherwise the arakoon ocamlbuild complains
PREFIX = "%s/%s" % (os.path.realpath(ROOT),'OCAML')

def sh(x, **kwargs): 
    print x
    subprocess.call(x,**kwargs)

def mr_proper():
    sh (['rm','-rf', ROOT])
    

print PREFIX
env = {'PATH': string.join([PREFIX + '/bin',
                            '/bin',
                            '/usr/bin',
                            ],':')}
def install_ocaml():
    cb = 'ocaml-%s' % OCAML
    archive = '%s.tar.bz2' % cb
    url = 'http://caml.inria.fr/pub/distrib/ocaml-3.12/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    sh(['wget', '-O', fn, url])
    sh(['tar', '-jxvf', archive], cwd = ROOT)
    d = '%s/%s' %(ROOT, cb)
    sh(['./configure', '--prefix',PREFIX], cwd = d)
    sh(['make','world.opt'], cwd = d)
    sh(['make','install'], cwd = d)

def install_ocamlfind():
    name = 'findlib-1.2.7'
    archive = name + '.tar.gz'
    url = 'http://download.camlcity.org/download/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    sh(['wget', '-O', fn, url])
    sh(['tar','-zxvf', archive], cwd = ROOT)
    d = '%s/%s' % (ROOT, name)
    sh(['./configure'], cwd = d , env = env)
    sh(['make','all','opt','install'], cwd = d, env = env)

def install_ounit():
    name = 'ounit-1.1.0'
    archive = name + '.tar.gz'
    url = 'http://forge.ocamlcore.org/frs/download.php/495/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    sh(['wget', '-O', fn, url])
    sh(['tar','-zxvf',archive ], cwd = ROOT)
    d = '%s/%s' % (ROOT, name)
    sh(['ocaml', 'setup.ml', '-configure'], cwd = d, env = env)
    sh(['ocaml', 'setup.ml', '-build'], cwd = d, env = env)
    sh(['ocaml', 'setup.ml', '-install'], cwd = d, env = env)
    
def install_react():
    name = 'react-0.9.2'
    archive =  name + '.tbz'
    url = 'http://erratique.ch/software/react/releases/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    
    sh(['wget', '-O', fn, url])
    sh(['tar','-jxvf', archive], cwd = ROOT)
    d = '%s/%s' % (ROOT, name)
    sh(['chmod','u+x','build'], cwd = d)
    sh(['./build'], cwd = d, env = env)
    sh(['./build','install'], cwd = d, env = env)
    sh(['mv', '%s/lib/ocaml/react' % PREFIX, '%s/lib/ocaml/site-lib/' % PREFIX])

def install_lwt():
    name = 'lwt-2.3.0'
    archive = name + '.tar.gz'
    url = 'http://ocsigen.org/download/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    sh(['wget', '-O', fn, url])
    sh(['tar','-zxvf', archive], cwd = ROOT)
    d = '%s/%s' % (ROOT, name)
    sh(['rm','-f',  'setup.data','setup.log'], cwd = d)
    sh(['make','clean'], cwd = d, env = env)
    sh(['ocaml', 'setup.ml', '-configure', '--prefix', PREFIX], cwd = d, env = env)
    sh(['ocaml', 'setup.ml', '-build'], cwd = d, env = env)
    sh(['ocaml', 'setup.ml', '-install'], cwd = d, env = env)

def install_camlbz2():
    name = 'camlbz2-0.6.0'
    archive = name + '.tar.gz'
    url = 'https://forge.ocamlcore.org/frs/download.php/72/%s' % archive
    fn = '%s/%s' % (ROOT, archive)
    sh(['wget', '-O', fn, url])
    sh(['tar', '-zxvf', archive], cwd = ROOT)
    d = '%s/%s' % (ROOT, name)
    sh(['./configure'], cwd = d, env = env)
    sh(['make', 'all'], cwd = d, env = env)
    sh(['make', 'install'], cwd = d, env = env)

def do_it():
    mr_proper()
    sh(['mkdir',ROOT])
    install_ocaml()
    install_ocamlfind()
    install_ounit()
    install_react()
    install_lwt()
    install_camlbz2()
    print '\n\nnow prepend %s/bin to your PATH' % PREFIX

if __name__ == '__main__':
    do_it()
