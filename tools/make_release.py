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

def update_redhat_spec(version):
    data = None
    fn = './redhat/SPECS/arakoon.spec'
    with open(fn,'r') as f:
        data = f.read()
    lines = data.split('\n')
    lines2 = []
    n = datetime.datetime.now()
    d = n.strftime("%a %b %d %Y")
    email = get_email()
    user = get_user()
    for line in lines:
        line_r = None
        if line.startswith('Version: '):
            line_r = "Version: %s" % version
        else:
            line_r = line
        lines2.append(line_r)
        if line_r == '%changelog':
            lines2.append("* %s %s <%s> - %s" % (d,user,email, version ))
            lines2.append("- Create arakoon %s RPM package" % version)

    data2 = "\n".join(lines2)
    with open(fn,'w') as f:
        f.write(data2)

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
    sh(['git', 'add', './META', './debian/changelog', './redhat/SPECS/arakoon.spec'])
    sh(['git', 'commit', '-m', 'make_release: %s' % version])

def make_release(version):
    make_branch(version)
    update_debian_changelog(version)
    update_redhat_spec(version)
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
