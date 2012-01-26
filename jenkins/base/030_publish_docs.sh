#!/bin/bash -eux

cd doc/_build
rm -rf publish
mkdir publish
cd publish
git clone git@github.com:Incubaid/arakoon.git
cd arakoon
git checkout -b gh-pages origin/gh-pages
cd documentation
rm -rf *
cd ..
cp -R ../../html/* documentation/
git add documentation

STATUS=`git status --porcelain | wc -l`
if [ "$STATUS" -eq 0 ];
then
    echo "Nothing to commit"
else
    git commit -a -m "Hudson: Update documentation from Mercurial source"
    git push origin gh-pages
fi

cd ../..
rm -rf publish
