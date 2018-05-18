#!/bin/bash

set -eux
set -o pipefail

function usage {
    echo "Usage: $(basename $0) <hadoop-version>"
}

if [[ $# -ne 2 ]]; then
    usage
    exit 1
fi

BRANCH=${1}
HADOOP_VERSION=${2}
case "${HADOOP_VERSION}" in
    "2.2.0" | "2.3.0" | "2.5.0" | "2.6.0" | "2.7.1" | "2.7.5" | "2.8.2")
        EXTRA_ARGS="-P hadoop2"
    ;;
    "3.0.1")
        EXTRA_ARGS="-P hadoop3"
    ;;
esac

echo "Install required packages"
sudo apt-get install -y maven openjdk-8-jdk-headless
mvn --version

echo "Build hadoop-openstack library"
pushd hadoop-swiftfs
mvn clean package ${EXTRA_ARGS:-} -Dhadoop.version=${HADOOP_VERSION}
mkdir -p ./../dist/hadoop-openstack/
mkdir -p ./../dist/hadoop-openstack/${BRANCH}
mv target/hadoop-openstack-3.0.0-SNAPSHOT.jar ./../dist/hadoop-openstack/${BRANCH}/hadoop-openstack-${HADOOP_VERSION}.jar
popd
