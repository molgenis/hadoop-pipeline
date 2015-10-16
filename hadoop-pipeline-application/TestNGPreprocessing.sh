#!/bin/env bash

# Bash script to copy and extract files from tools.tar.gz to
# target/test-classes/ for TestNG tests to work correctly.

cd ./target/test-classes/
cp ../../../tools.tar.gz ./
tar -zxvf tools.tar.gz
cd ../../
