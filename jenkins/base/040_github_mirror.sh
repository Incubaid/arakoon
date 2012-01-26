#!/bin/bash -eux
echo "Updating GitHub mirror"

hg pull

MIRROR=/tmp/arakoon_github_mirror
SRC=`pwd`
FAST_EXPORT=/tmp/fast-export

test -d $FAST_EXPORT || (cd `dirname $FAST_EXPORT`; git clone git://repo.or.cz/fast-export.git)

rm -rf "$MIRROR"
mkdir "$MIRROR"
cd "$MIRROR"

git init arakoon.git
cd arakoon.git
$FAST_EXPORT/hg-fast-export.sh --force -r "$SRC"
git checkout master
git push --all git@github.com:Incubaid/arakoon.git
git push --tags git@github.com:Incubaid/arakoon.git

cd "$SRC"
rm -rf "$MIRROR"
