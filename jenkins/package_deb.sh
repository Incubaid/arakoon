#!/bin/bash -xue

DEB_BUILD_OPTIONS="nostrip nocheck" fakeroot debian/rules clean build binary
cp ../*.deb .
