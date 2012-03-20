import subprocess
import argparse
import datetime

def sh(x,**kwargs):
    if subprocess.call(x,**kwargs):
        raise RuntimeError("Failed: %s" % (string.join(x,' ')))

def update_debian_changelog(version):
    data = None
    fn = 'debian/changelog'
    with open(fn,'r') as f:
        data = f.read()
    template = """arakoon (%s) unstable; urgency=low
  * new upstream version

 -- Romain Slootmaekers <romain@incubaid.com>  %s

"""
    n = datetime.datetime.now()
    d = n.strftime("%a, %d %b %Y %H:%M:%S +0100") # is TZ always +0100 ?
    data = template % (version, d) + data
    with open(fn,'w') as f:
        f.write(data)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', required = True)
    options = parser.parse_args()
    version = options.version

    id_line = subprocess.Popen(['hg', 'id','-i'], 
                               stdout = subprocess.PIPE).communicate()[0]
    id = id_line.strip()
    if id.endswith('+'):
        raise RuntimeError("repo should be clean")
    
    sh(['hg','branch',version])
    update_debian_changelog(version)
    sh(['hg','commit', '-m', "%s and debian/changelog" % version])
    print "you can push if you're satisfied"
