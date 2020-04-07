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
        HADOOP_VERSION="2.7.1"
        BUILD_ARGS="-Puber -P hadoop-2"
    ;;
    "2.7.5")
        OOZIE_VERSION="4.3.0"
        HADOOP_VERSION="2.7.5"
        BUILD_ARGS="-Puber -P hadoop-2"
    ;;
    "2.8.2")
        OOZIE_VERSION="4.3.0"
        HADOOP_VERSION="2.8.2"
        BUILD_ARGS="-Puber -P hadoop-2"
    ;;
    "3.0.1")
        OOZIE_VERSION="5.0.0"
        HADOOP_VERSION="3.0.1"
        BUILD_ARGS="-Puber "

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
if [ "${OOZIE_VERSION}" = "4.2.0" ]; then
    # see https://issues.apache.org/jira/browse/OOZIE-2417
    mv pom.xml pom.xml.orig
    xmlstarlet ed -P -N N="http://maven.apache.org/POM/4.0.0" -d "/N:project/N:repositories/N:repository[N:url='http://repository.codehaus.org/']" pom.xml.orig >pom.xml
fi
if [ "${OOZIE_VERSION}" = "4.3.0" ]; then
    # see https://issues.apache.org/jira/browse/OOZIE-2533
    patch -p0 < ./../tools/oozie_webUI.patch

    # add commons-httpclient as a dependency to sharelib/oozie
    pushd sharelib/oozie
    mv pom.xml pom.xml.orig
    xmlstarlet ed -N N="http://maven.apache.org/POM/4.0.0" --subnode "/N:project/N:dependencies" -t elem -n dependency -v '' pom.xml.orig > pom.xml.tmp
    xmlstarlet ed -P -N N="http://maven.apache.org/POM/4.0.0" \
        --subnode "/N:project/N:dependencies/N:dependency[last()]" -t elem -n groupId -v commons-httpclient \
        --subnode "/N:project/N:dependencies/N:dependency[last()]" -t elem -n artifactId -v commons-httpclient \
        --subnode "/N:project/N:dependencies/N:dependency[last()]" -t elem -n version -v 3.1 \
        --subnode "/N:project/N:dependencies/N:dependency[last()]" -t elem -n scope -v compile pom.xml.tmp > pom.xml
    popd
fi
if [ "${OOZIE_VERSION}" = "5.0.0" ]; then
    # see https://issues.apache.org/jira/browse/OOZIE-3219
    patch -p0 < ./../tools/oozie_core.patch
fi

mv pom.xml pom.xml.tmp
xmlstarlet ed -P -N N="http://maven.apache.org/POM/4.0.0" -u "/N:project/N:repositories/N:repository[N:url='http://repo1.maven.org/maven2']/N:url" -v "https://repo1.maven.org/maven2" pom.xml.tmp >pom.xml

./bin/mkdistro.sh assembly:single ${BUILD_ARGS} -Dhadoop.version=${HADOOP_VERSION} -DjavaVersion=1.8 -DtargetJavaVersion=1.8 -DskipTests
mkdir -p ./../dist/oozie/
mv distro/target/oozie-${OOZIE_VERSION}-distro.tar.gz ./../dist/oozie/oozie-${OOZIE_VERSION}-hadoop-${HADOOP_VERSION}.tar.gz
popd
rm -rf oozie-${OOZIE_VERSION}/ oozie-${OOZIE_VERSION}.tar.gz
