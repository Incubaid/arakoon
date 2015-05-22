import subprocess
import argparse
import datetime

def sh(x,**kwargs):
    if subprocess.call(x,**kwargs):
        raise RuntimeError("Failed: %s" % (string.join(x,' ')))

def get_email():
    r = subprocess.check_output(['git','config','user.email'])
    r = r.strip()
    return r

def get_user():
    r = subprocess.check_output(['git','config','user.name'])
    r = r.strip()
    return r

def update_debian_changelog(version):
    data = None
    fn = './debian/changelog'
    with open(fn,'r') as f:
        data = f.read()
    template = """arakoon (%s) unstable; urgency=low
  * new upstream version

 -- %s <%s>  %s

"""
    n = datetime.datetime.now()
    d = n.strftime("%a, %d %b %Y %H:%M:%S +0100") # is TZ always +0100 ?
    user = get_user()
    email = get_email()
    data = template % (version, user, email, d) + data
    with open(fn,'w') as f:
        f.write(data)

def update_meta(version):
    fn = './META'
    with open(fn,'r') as f :
        data = f.read()
    lines = data.split('\n')
    i = 0
    for line in lines:
        if line.startswith('version'):
            line = 'version = "%s"' % version
            lines[i] = line
        i += 1
    data2 = "\n".join(lines)
    with open(fn,'w') as f:
        f.write(data2)

def tag(version):
    sh(['git','tag', '-a', version, '-m', version])

def push(version):
    sh(['git','push', '-v', 'origin', version])

def delete_tag(version):
    sh(['git', 'tag', '-d', version])
    #sh(['git', 'push', 'origin', ':refs/tags/%s' % version])

def make_branch(version):
    sh(['git','checkout', '-b', version])

def add_commit(version):
    sh(['git', 'add', './META', './debian/changelog'])
    sh(['git', 'commit', '-m', 'make_release: %s' % version])

def make_release(version):
    make_branch(version)
    update_debian_changelog(version)
    update_meta(version)
    add_commit(version)
    push(version)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', required = True,
                        help = "for example 3.1.0")
    options = parser.parse_args()
    version = options.version
    make_release(version)
