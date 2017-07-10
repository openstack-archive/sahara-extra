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
    "2.7.1")
        OOZIE_VERSION="4.2.0"
        PREV_OOZIE_HADOOP_VERSION="2.3.0"
        HADOOP_VERSION="2.7.1"
        BUILD_ARGS="-Puber -P hadoop-2"
    ;;
esac

echo "Install required packages"
sudo apt-get purge -y maven2 maven
sudo apt-get install -y maven openjdk-8-jdk wget xmlstarlet
mvn --version

echo "Download and unpack Oozie"
wget http://archive.apache.org/dist/oozie/${OOZIE_VERSION}/oozie-${OOZIE_VERSION}.tar.gz
tar xzf oozie-${OOZIE_VERSION}.tar.gz

echo "Build Oozie"
pushd oozie-${OOZIE_VERSION}
find . -name pom.xml | xargs sed -ri "s/${PREV_OOZIE_HADOOP_VERSION}/${HADOOP_VERSION}/g"
if [ "${OOZIE_VERSION}" = "4.2.0" ]; then
    # see https://issues.apache.org/jira/browse/OOZIE-2417
    mv pom.xml pom.xml.orig
    xmlstarlet ed -P -N N="http://maven.apache.org/POM/4.0.0" -d "/N:project/N:repositories/N:repository[N:url='http://repository.codehaus.org/']" pom.xml.orig >pom.xml
fi
./bin/mkdistro.sh assembly:single ${BUILD_ARGS} -DjavaVersion=1.7 -DtargetJavaVersion=1.7 -DskipTests
mkdir -p ./../dist/oozie/
mv distro/target/oozie-${OOZIE_VERSION}-distro.tar.gz ./../dist/oozie/oozie-${OOZIE_VERSION}-hadoop-${HADOOP_VERSION}.tar.gz
popd
rm -rf oozie-${OOZIE_VERSION}/ oozie-${OOZIE_VERSION}.tar.gz
