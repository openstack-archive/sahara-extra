#!/bin/bash

set -eux
set -o pipefail

echo "Building all needed oozie versions"
./tools/build-oozie.sh 1.2.1
./tools/build-oozie.sh 2.6.0

echo "Building all needed hadoop-openstack versions"
hadoop_versions=(2.2.0 2.3.0 2.5.0 2.6.0 2.7.1)
for ver in ${hadoop_versions[*]}
do
    ./tools/build-hadoop-openstack.sh ${ver}
done
