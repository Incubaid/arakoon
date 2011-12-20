import os
import os.path
import logging

import jinja2

import feedparser

URI = 'http://blog.incubaid.com/category/arakoon/feed/'
ENCODING = 'utf-8'
FILTER = lambda f: f.endswith('.html') and not f.startswith('_')

LOGGER = logging.getLogger(__name__)

def run(src, target):
    LOGGER.info('Rendering %s to %s', src, target)

    context = {}

    feed = feedparser.parse(URI)
    context['feed'] = feed

    loader = jinja2.FileSystemLoader((src, ))
    environment = jinja2.Environment(
        loader=loader, undefined=jinja2.StrictUndefined)

    for name in filter(FILTER, os.listdir(src)):
        LOGGER.debug('Rendering %s', name)

        template = environment.get_template(name)

        output = template.render(context)
        output_str = output.encode(ENCODING)

        out = os.path.join(target, name)
        fd = open(out, 'w')
        try:
            LOGGER.debug('Writing to %s', out)

            fd.write(output_str)
        finally:
            fd.close()

def main():
    src = os.path.abspath(os.path.dirname(__file__))
    target = os.path.abspath(os.path.join(src, os.path.pardir))

    run(src, target)

if __name__ == '__main__':
    main()
