import os
import os.path
import logging

import lxml.html

import jinja2

import pygments.formatters
import pygments.styles

import rst_directive # Imported for side-effects, keep in place

import docutils
import docutils.core

CODE_CSS_STYLE = 'trac'
CODE_CSS_PREFIX = 'div.highlight'

ENCODING = 'utf-8'
FILTER = lambda f: \
    (f.endswith('.html') or f.endswith('.rst')) \
    and not (os.path.basename(f).startswith('_'))

RST_SETTINGS = {
    'initial_header_level': 2,
}
RST_TEMPLATE = u'''
{%% extends '_base.html' %%}
{%% block title %%}%(title)s{%% endblock %%}
{%% block main %%}
%(main)s
{%% endblock %%}
'''

LOGGER = logging.getLogger(__name__)

def render_rst(in_, out):
    fd = open(in_, 'r') # Will be closed by docutils

    text, doc = docutils.core.publish_programmatically(
        source_class=docutils.io.FileInput, source=fd, source_path=in_,
        destination_class=docutils.io.StringOutput, destination=None,
        destination_path=None,
        reader=None, reader_name='standalone',
        parser=None, parser_name='restructuredtext',
        writer=None, writer_name='html',
        settings=None, settings_spec=None, settings_overrides=RST_SETTINGS,
        config_section=None, enable_exit_status=False)

    title = doc.document['title']

    document = lxml.html.fromstring(text)

    # Convert ReST 'warning' and 'info' structures into corresponding Bootstrap
    # markup
    for (orig, new_) in (('important', 'warning'), ('tip', 'info'), ):
        blocks = document.cssselect('div.%s' % orig)

        for block in blocks:
            classes = block.attrib['class']
            new_classes = 'alert-message block-message %s %s' % (new_, classes)
            block.attrib['class'] = new_classes

            titles = block.cssselect('p.admonition-title')

            assert len(titles) <= 1

            if titles:
                title_elem = titles[0]

                assert title_elem.getchildren() == []

                elem = lxml.html.fragment_fromstring('<strong>')
                elem.text = title_elem.text
                title_elem.text = ''
                title_elem.append(elem)

    body_html = lxml.html.tostring(document.cssselect('div.document')[0])

    # Bleh...
    body_html = body_html.replace('src="%7B%7Bbase%7D%7D', 'src="{{ base }}')

    template = RST_TEMPLATE % {
        'title': title,
        'main': body_html,
    }

    data = template.encode('utf-8')
    fd_out = open(out, 'w')
    try:
        fd_out.write(data)
    finally:
        fd_out.close()

def create_dirs(base, name):
    path = os.path.dirname(name)
    target = os.path.join(base, path)

    if not os.path.isdir(target):
        if os.path.exists(target):
            raise RuntimeError('File "%s" exists' % target)

        os.makedirs(target, 0755)

def run(src, target):
    LOGGER.info('Rendering %s to %s', src, target)

    context = {}

    loader = jinja2.FileSystemLoader((src, ))
    environment = jinja2.Environment(
        loader=loader, undefined=jinja2.StrictUndefined)

    for name in filter(FILTER,
        (os.path.relpath(os.path.join(d, f), start=src)
            for (d, _, fs) in os.walk(src)
            for f in fs)):

        LOGGER.debug('Rendering %s', name)

        create_dirs(target, name)

        cleanup_html = False
        html_file = None

        template_name = name

        if name.endswith('.rst'):
            cleanup_html = True

            basename = name[:-4]
            template_name = '%s.html' % basename
            html_file = os.path.join(src, template_name)
            assert not os.path.exists(html_file)

            render_rst(os.path.join(src, name), html_file)

        try:
            template = environment.get_template(template_name)

            local_context = context.copy()
            local_context['name'] = os.path.splitext(template_name)[0] \
                    .replace('/', '_') \
                    .replace('.', '_')

            base_rel = os.path.relpath(src, os.path.dirname(name))
            if base_rel == os.path.curdir:
                local_context['base'] = ''
            else:
                local_context['base'] = '%s/' % base_rel

            output = template.render(local_context)
            output_str = output.encode(ENCODING)

            out = os.path.join(target, template_name)
            fd = open(out, 'w')
            try:
                LOGGER.debug('Writing to %s', out)

                fd.write(output_str)
            finally:
                fd.close()
        finally:
            if cleanup_html:
                os.unlink(html_file)

    style = pygments.styles.get_style_by_name(CODE_CSS_STYLE)
    formatter = pygments.formatters.get_formatter_by_name('html', style=style)

    code_style_css = os.path.join(target, 'code.css')
    code_style_fd = open(code_style_css, 'w')
    try:
        code_style_fd.write('/* Pygments style "%s" */\n' % CODE_CSS_STYLE)
        code_style_fd.write(formatter.get_style_defs(CODE_CSS_PREFIX))
    finally:
        code_style_fd.close()

def main():
    src = os.path.abspath(os.path.dirname(__file__))
    target = os.path.abspath(os.path.join(src, os.path.pardir))

    run(src, target)

if __name__ == '__main__':
    if 'DEBUG' in os.environ:
        logging.basicConfig(level=logging.DEBUG)

    main()
