#!/bin/bash

set -eux
set -o pipefail

echo "Building all needed oozie versions"
./tools/build-oozie.sh 1.2.1
./tools/build-oozie.sh 2.6.0

echo "Building all needed hadoop-openstack versions"
./tools/build-hadoop-openstack.sh 1.2.1
./tools/build-hadoop-openstack.sh 2.6.0
