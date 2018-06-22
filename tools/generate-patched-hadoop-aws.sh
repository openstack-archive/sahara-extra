#!/bin/bash

set -e

# First, install jdk, maven, git, etc.

for version in 2.7.1 2.7.3 2.7.5; do
    git clone git://git.apache.org/hadoop.git -b branch-$version --depth 1
    cd hadoop/hadoop-tools/hadoop-aws/
    git apply ../../../path-style-access.patch
    mvn package -Dhadoop.version=$version
    mv target/hadoop-aws-$version.jar ../../../
    cd ../../../
    rm -rf hadoop
done

ls hadoop-aws-*.jar
