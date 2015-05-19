Mini Accumulo Cluster Example
=======

Introduction
-----------

Instamo makes it easy to write some code and run it against a local, transient
[Accumulo](http://accumulo.apache.org) instance in minutes.  No setup or
installation is required.  This is possible if Java and Maven are already
installed by following the steps below.

```
vim src/test/java/instamo/ExampleAccumuloUnitTest.java
mvn package
```

The maven package command will run the unit test.  After packing the code, you
can also run one of the below applications.

Map Reduce
----------

It's possible to run local map reduce jobs against the MiniAccumuloCluster
instance.   There is an example of this in the src directory  The following
command will run the map reduce example.

```
mvn exec:exec -P mapreduce
```

Accumulo Shell
-----------

The Accumulo shell is a simple application that, among other features, provides
interactive access to tables in Accumulo. The following command will launch the
shell against a local Accumulo instance.

```
mvn exec:exec -P shell
```

TODO: clean this up
