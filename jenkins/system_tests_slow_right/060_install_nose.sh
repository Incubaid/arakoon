echo "Install nose utilities in QBase"

test -f /tmp/coverage-3.4b2.tar.gz || wget -q -O /tmp/coverage-3.4b2.tar.gz http://pypi.python.org/packages/source/c/coverage/coverage-3.4b2.tar.gz
sudo /opt/qbase3/bin/easy_install /tmp/coverage-3.4b2.tar.gz

test -f /tmp/nosexcover-1.0.4.tar.gz || wget -q -O /tmp/nosexcover-1.0.4.tar.gz http://pypi.python.org/packages/source/n/nosexcover/nosexcover-1.0.4.tar.gz
sudo /opt/qbase3/bin/easy_install /tmp/nosexcover-1.0.4.tar.gz

sudo rm -f ${WORKSPACE}/coverage.xml
sudo rm -f /opt/qbase3/var/tests/coverage.xml

cd ${WORKSPACE}
mkdir -p _tools
cd _tools
rm -f fix_coverage.py

cat > fix_coverage.py << EOF
import sys
import os
import os.path
import xml.sax
import xml.sax.saxutils

class CoverageFixHandler(xml.sax.saxutils.XMLGenerator):
    def __init__(self, base, prefixes, *args, **kwargs):
        xml.sax.saxutils.XMLGenerator.__init__(self, *args, **kwargs)

        self._base = base
        self._prefixes = tuple(
            sorted(prefixes, key=lambda s: len(s), reverse=True))

    def startElement(self, name, attrs):
        attrs = dict(attrs.items())

        # Note: can't use 'in' !
        if 'filename' in attrs:
            filename = attrs['filename']

            if filename.startswith('/'):
                # Convert into relative path
                relative_path = os.path.relpath(filename, self._base)

                attrs['filename'] = relative_path

        if name == 'package' and 'name' in attrs:
            package = attrs['name']

            for prefix in self._prefixes:
                if package.startswith(prefix):
                    attrs['name'] = package[len(prefix):]
                    break

        return xml.sax.saxutils.XMLGenerator.startElement(self, name, attrs)


def fix(base, prefixes, in_, out):
    xml.sax.parse(in_, CoverageFixHandler(base, prefixes, out))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('Missing base argument\n')
        sys.exit(1)

    fix(sys.argv[1], sys.argv[2:], sys.stdin, sys.stdout)

EOF
