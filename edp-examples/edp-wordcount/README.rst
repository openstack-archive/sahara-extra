=====================
EDP WordCount Example
=====================
Overview
========

``WordCount.java`` is a modified version of the WordCount example bundled with
version 1.2.1 of Apache Hadoop. It has been extended for use from a java action
in an Oozie workflow. The modification below allows any configuration values
from the ``<configuration>`` tag in an Oozie workflow to be set in the Configuration
object::

    // This will add properties from the <configuration> tag specified
    // in the Oozie workflow.  For java actions, Oozie writes the
    // configuration values to a file pointed to by ooze.action.conf.xml
    conf.addResource(new Path("file:///",
                              System.getProperty("oozie.action.conf.xml")));

In the example workflow, we use the ``<configuration>`` tag to specify user and
password configuration values for accessing swift objects.

Compiling
=========

To build the jar, add ``hadoop-core`` and ``commons-cli`` to the classpath.

On a node running Ubuntu 13.04 with hadoop 1.2.1 the following commands
will compile ``WordCount.java`` from within the ``src`` directory::

$ mkdir wordcount_classes
$ javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar:/usr/share/hadoop/lib/commons-cli-1.2.jar -d wordcount_classes WordCount.java
$ jar -cvf edp-wordcount.jar -C wordcount_classes/ .

(A compiled ``edp-wordcount.jar`` is included in ``wordcount/lib``. Replace it if you rebuild)

Running from the command line with Oozie
========================================

The ``wordcount`` subdirectory contains a ``job.properties`` file, a ``workflow.xml`` file,
and a ``lib`` directory with an ``edp-wordcount.jar`` compiled as above.

To run this example from Oozie, you will need to modify the ``job.properties`` file
to specify the correct ``jobTracker`` and ``nameNode`` addresses for your cluster.

You will also need to modify the ``workflow.xml`` file to contain the correct input
and output paths. These paths may be Sahara swift urls or hdfs paths. If swift
urls are used, set the ``fs.swift.service.sahara.username`` and ``fs.swift.service.sahara.password``
properties in the ``<configuration>`` section.

1) Upload the ``wordcount`` directory to hdfs

  ``$ hadoop fs -put wordcount wordcount``

2) Launch the job, specifying the correct oozie server and port

  ``$ oozie job -oozie http://oozie_server:port/oozie -config wordcount/job.properties -run``

3) Don't forget to create your swift input path!  A Sahara swift url looks like *swift://container.sahara/object*

Running from the Sahara UI
===========================

Running the WordCount example from the Sahara UI is very similar to running a Pig, Hive,
or MapReduce job.

1) Create a job binary that points to the ``edp-wordcount.jar`` file
2) Create a ``Java`` job type and add the job binary to the ``libs`` value
3) Launch the job:


   a) Add the input and output paths to ``args``
   b) If swift input or output paths are used, set the ``fs.swift.service.sahara.username`` and ``fs.swift.service.sahara.password``
      configuration values
   c) The Sahara UI will prompt for the required ``main_class`` value and the optional ``java_opts`` value


