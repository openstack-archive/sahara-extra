======================================================
Sources for Swift filesystem implementation for Hadoop
======================================================

These sources were originally published at
https://issues.apache.org/jira/secure/attachment/12583703/HADOOP-8545-033.patch
The sources were obtained by running "patch" command. All the files related to
Hadoop-common were skipped during patching.

Changes were made after patching:
* pom.xml was updated to use hadoop-core 1.1.2 dependency and adds hadoop2 
  profile
* removed dependency on 2.x hadoop in code (@Override and isDirectory() 
  -> isDir())
* removed Hadoop 2.X tests

There are no unit-tests, only integration.
