#!/bin/bash

pushd _build
bisect-report.opt -html report ../bisect0001.out
bisect-report.opt -xml-emma report/coverage-arakoon.xml ../bisect0001.out
popd
rm bisect0001.out
