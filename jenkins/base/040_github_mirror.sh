#!/bin/bash -e -u -x
echo "Updating GitHub mirror"

hg pull

MIRROR=/tmp/arakoon_github_mirror
SRC=`pwd`

rm -rf "$MIRROR"
mkdir "$MIRROR"
cd "$MIRROR"

git init arakoon.git
cd arakoon.git
/home/hudson/nicolas/fast-export/hg-fast-export.sh --force -r "$SRC"
git checkout master
git push --all git@github.com:Incubaid/arakoon.git
git push --tags git@github.com:Incubaid/arakoon.git

cd "$SRC"
rm -rf "$MIRROR"
