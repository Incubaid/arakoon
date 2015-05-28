#!/bin/bash -xue

eval `opam config env`

env DEB_BUILD_OPTIONS=nocheck fakeroot debian/rules clean
env DEB_BUILD_OPTIONS=nocheck fakeroot debian/rules build
env DEB_BUILD_OPTIONS=nocheck fakeroot debian/rules binary
