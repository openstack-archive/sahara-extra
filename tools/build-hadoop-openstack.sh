#!/bin/bash

set -eux
set -o pipefail

function usage {
    echo "Usage: $(basename $0) <plugin-version>"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

PLUGIN_VERSION=${1}
case "${PLUGIN_VERSION}" in
    "2.6.0")
        EXTRA_ARGS="-P hadoop2"
    ;;
esac

echo "Install required packages"
sudo apt-get install -y maven openjdk-7-jdk
mvn --version

echo "Build hadoop-openstack library"
pushd hadoop-swiftfs
mvn clean package ${EXTRA_ARGS:-} -Dhadoop.version=${PLUGIN_VERSION}
mkdir -p ./../dist/hadoop-openstack/
mv target/hadoop-openstack-3.0.0-SNAPSHOT.jar ./../dist/hadoop-openstack/hadoop-openstack-${PLUGIN_VERSION}.jar
popd
