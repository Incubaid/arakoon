Getting Started
===============
Once you cloned the repository, set up a branch to track the upstream site::

    git checkout -b gh-pages origin/gh-pages

Now, initialize and update the git submodules::

    git submodule init
    git submodule update

Finally, install the required dependencies:

- Jinja2_, a Python template engine
- FeedParser_, a Python library for RSS/Atom/... feed parsing

.. _Jinja2: http://jinja.pocoo.org/
.. _FeedParser: http://code.google.com/p/feedparser/

Editing
=======
.. caution:: Never edit HTML files in the root of the repository manually.
    These will be overwritten by the build process. You can edit CSS files etc.

The site is built using Jinja2 templates from files in the ``src`` folder. Any
file with the ``html`` extension, except for the ones starting with an
underscore character, will be rendered into the corresponding HTML file in the
root directory.

Simply run the ``build.py`` script to regenerate all pages.

Please use one commit to change files in the ``src`` folder, or any CSS or other
files, and finally one commit to change all HTML files in the root folder after
rebuilding them. Make sure to preview the generated HTML files in your local
browser before pushing the changes: the updated files will be displayed on the
website immediately after pushing them.
