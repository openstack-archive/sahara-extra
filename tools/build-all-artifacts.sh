#!/bin/bash

set -eux
set -o pipefail
export BRANCH=$1
export BRANCH=${BRANCH:-master}

echo "Building all needed hadoop-openstack versions"
hadoop_versions=(2.2.0 2.3.0 2.5.0 2.6.0 2.7.1 2.7.5 2.8.2 3.0.1)
for ver in ${hadoop_versions[*]}
do
    ./tools/build-hadoop-openstack.sh $BRANCH ${ver}
done

echo "Building all oozie versions"
hadoop_oozie_versions=(2.7.1 2.7.5 2.8.2 3.0.1)
for ver in ${hadoop_oozie_versions[*]}
do
    ./tools/build-oozie.sh ${ver}
done

cp -r common-artifacts/ dist/
ls dist/
