=======================
Sources for main function wrapper that adapt for oozie
=======================

In order to pass configurations to MapReduce Application through oozie,
it is necessary to add the following code.
(https://opendev.org/openstack/sahara-tests/src/branch/master/sahara_tests/scenario/defaults/edp-examples/edp-java/README.rst)

    // This will add properties from the <configuration> tag specified
    // in the Oozie workflow.  For java actions, Oozie writes the
    // configuration values to a file pointed to by ooze.action.conf.xml
    conf.addResource(new Path("file:///",
                              System.getProperty("oozie.action.conf.xml")));

This wrapper adds a above configuration file to a default resources and
invoke actual main function.

And this wrapper provides workaround for oozie's System.exit problem.
(https://oozie.apache.org/docs/4.0.0/WorkflowFunctionalSpec.html#a3.2.7_Java_Action)
In caller of oozie, System.exit is converted to exception.
The application can call System.exit multiple times.

This wrapper stores the argument of System.exit called in first.
And return stored value if System.exit is called multiple times.
