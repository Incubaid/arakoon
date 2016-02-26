#!/bin/bash

set -e

env | sort

git submodule init
git submodule update
