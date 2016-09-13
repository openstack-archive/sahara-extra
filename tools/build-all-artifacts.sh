#!/bin/bash

set -eux
set -o pipefail
export BRANCH=$1
export BRANCH=${BRANCH:-master}

echo "Building all needed hadoop-openstack versions"
hadoop_versions=(2.2.0 2.3.0 2.5.0 2.6.0 2.7.1)
for ver in ${hadoop_versions[*]}
do
    ./tools/build-hadoop-openstack.sh $BRANCH ${ver}
done

cp -r common-artifacts/ dist/
ls dist/
