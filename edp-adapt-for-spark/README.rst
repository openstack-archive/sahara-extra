===========================================
Sources for main function wrapper for Spark
===========================================

The Hadoop configuration for a Spark job must be modified if
the Spark job is going to access Swift paths.  Specifically,
the Hadoop configuration must contain values that allow
the job to authenticate to the Swift service.

This wrapper adds a specified xml file to the default Hadoop
Configuration resource list and then calls the specified
main class. Any necessary Hadoop configuration values can
be added to the xml file.  This allows the main class to
be run and access Swift paths without alteration.
