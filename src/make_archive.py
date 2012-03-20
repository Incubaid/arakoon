import os
import re

template = """
===============
Arakoon Archive
===============

Releases
========
{releases_list}

{releases_links}
"""

version_tmpl = '- {version}_'
link_tmpl = '.. _{version}: {version}.html'

def get_releases():
    rel = list()

    def get_version(fn):
        m = re.match('(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\.rst', fn)
        if m:
            return dict(version='.'.join(m.groups()))

    def f(arg, d, fs):
        if d == 'releases':
            rel.extend(filter(None, map(get_version, fs)))

    os.path.walk('releases', f, None)

    return sorted(rel, reverse=True, key=lambda x: x['version'])

releases = get_releases()

releases_list = '\n'.join(map(lambda x: version_tmpl.format(**x), releases))
releases_links = '\n'.join(map(lambda x: link_tmpl.format(**x), releases))


print template.format(releases_list=releases_list,
        releases_links=releases_links)




