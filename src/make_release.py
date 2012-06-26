import argparse
import httplib
import datetime
import re
import string

from BeautifulSoup import BeautifulSoup
from HTMLParser import HTMLParser

class JiraClient(object):
    """Temporary HTTP JIRA client as we can't get release notes from the API
    """

    def __init__(self):
        self._project_id = 10000
        self._cookie = None
        self._connection = None
        self._parser = HTMLParser()

    def connect(self, domain, login=None, password=None):
        self._cookie = None
        self._connection = httplib.HTTPConnection(domain)
        _, headers = self._get_content('/')
        self._cookie = [h for h in headers if h[0] == 'set-cookie'][0]

        if not self._cookie:
            raise ValueError('Failed to connect to domain %s' % domain)

        if login and password:
            self._login(login, password)

    def get_versions(self):
        path = \
        "/browse/ARAKOON?selectedTab=com.atlassian.jira.plugin.system.project%3Aversions-panel"

        data, _ = self._get_content(path)

        soup = BeautifulSoup(data)
        version_table = soup.find(name='table', attrs={'id': 'versions_panel'})
        rows = version_table.findChildren('tr')

        def tr_to_version(tr):
            rd = tr.findChild(name='span', attrs={'class':
                'dtstart dtend hidden'})
            release_date = datetime.datetime.strptime(rd.text, '%Y-%m-%dT%H-%M') \
                           if rd \
                           else None
            description = tr.findChild(name='span', attrs={'class':
                'description'}).text
            version_id = tr.findChild(name='a', attrs={'class':
                'summary'})['id'][len('version_'):]
            version = tr.findChild(name='a', attrs={'class':
                'summary'}).text

            return (version,
                    {'version': self._clean(version),
                     'version_id': self._clean(version_id),
                     'description': self._clean(description),
                     'release_date': release_date,
                     })

        r = dict(map(tr_to_version, rows))
        return r 

    def get_version_info(self, version):
        versions = self.get_versions()
        v = versions.get(version, None)
        if not v:
            raise ValueError('Invalid version specified')
        return v

    def get_release_info(self, version):
        v = self.get_version_info(version)
        version_id = v['version_id']

        path = \
        "/secure/ReleaseNote.jspa?version=%(version)s&projectId=%(project)s" % \
            {'version': version_id, 'project': self._project_id}

        data, _ = self._get_content(path)

        soup = BeautifulSoup(data)
        start = soup.findChild(name='a', text='Configure Release Notes')

        if not start:
            raise ValueError('Failed to parse release notes')

        p = re.compile("^\[(?P<KEY>[A-Z0-9\-]+)\] - (?P<MSG>.+)$")

        def filter_issue(l):
            issue =  re.match(p, l.text).groupdict()
            issue['MSG'] = self._clean(issue['MSG'])
            return issue

        n = start.findParent().findNext('h2')
        notes = dict()
        while n:
            if n.name.upper() == 'H2':
                section = n.text
                n = n.findNextSibling('ul')
            elif n.name.upper() == 'UL':
                issues = [filter_issue(l) for l in
                        n.findChildren('li')]
                notes[section] = issues
                n = n.findNextSibling('h2')
        return notes


    def _login(self, login, password):
        d = "os_username=%(l)s&os_password=%(p)s&os_destination=%%2Fsecure%%2F"
        path = '/login.jsp'
        data = d % {'l': login, 'p': password}
        headers = {"Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": str(len(data)) }
        r = self._req('POST', path, data, headers)
        r.read()

    def _clean(self, html):
        return self._parser.unescape(html)

    def _get(self, path):
        return self._req('GET', path)

    def _get_content(self, path):
        response = self._get(path)
        data = response.read()
        headers = response.getheaders()
        return data, headers

    def _req(self, req_type, path, data=None, headers=None):
        if not self._connection:
            raise RuntimeError('Not connected')

        if not headers:
            headers = {}

        headers['Cookie'] = self._cookie
        self._connection.request(req_type, path, data, headers)
        return self._connection.getresponse()


template = """
==============
Arakoon {version}
==============

Published on: {release_date}

Release
=======
+----------------+------------------+----------------+--------------+
| QPackage name  | QPackage version | QPackage build |   Revision   |
+================+==================+================+==============+
| arakoon        | {version:^16} |       0        | {revision:>11} |
+----------------+------------------+----------------+--------------+
| arakoon_client | {version:^16} |       0        | {revision:>11} |
+----------------+------------------+----------------+--------------+

What's new
==========
{features}

What's fixed
============
{bugs}

{links}
"""

parser = argparse.ArgumentParser()
parser.add_argument('--version', required=True)
parser.add_argument('--rev', required=True)

options = parser.parse_args()

version = options.version
revision = options.rev

domain = 'jira.incubaid.com'

jira = JiraClient()
jira.connect(domain)

version_info = jira.get_version_info(version)
release_info = jira.get_release_info(version)


f = string.Formatter()

def get_issue_list(category, info):
    issues = info.get(category, None)

    if not issues:
        return 'No changes.'

    def issue_to_row(i):

        key = i['KEY'].strip() + '_'
        msg = i['MSG']

        if len(msg) > 64:
            msg =  msg[:61] + '...'


        fmt = """| {key:<12} | {msg:<64} |
+--------------+------------------------------------------------------------------+"""
        return f.format(fmt, key=key, msg=msg)

    l = '\n'.join(map(issue_to_row, issues))

    header = """
+--------------+------------------------------------------------------------------+
| Jira key     | Comment                                                          |
+==============+==================================================================+
"""
    return header + l


def get_links(info):
    links = []

    def link(i):
        return '.. _%(KEY)s:  http://jira.incubaid.com/browse/%(KEY)s' % i

    for k, v in info.iteritems():
        links.extend(map(link, v))

    return '\n'.join(links)

bugs = get_issue_list('Bug', release_info)
features = get_issue_list('New Feature', release_info)
links = get_links(release_info)

params = {
        'version': version,
        'release_date': version_info['release_date'].date(),
        'revision': revision,
        'bugs': bugs,
        'features': features,
        'links': links,
        }



release_notes = f.format(template, **params)

print release_notes
