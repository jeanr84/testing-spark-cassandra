# testing-spark-cassandra

The goal of this project is to present on a simple use case how to quickly test a Spark job based on Cassandra data 
without installing and configuring a Spark or a Cassandra cluster.

### Versions

* Scala : 2.11.12
* SBT : 1.2.1
* Spark : 2.3.0
* Cassandra : 3.2

### Build

Here is the list of the dependencies used to build this project

```
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "2.3.0" % Test,
  "org.apache.cassandra" % "cassandra-all" % "3.2" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.10.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalacheck" %% "scalacheck" % "1.10.0" % Test
).map(_.exclude("org.slf4j", "log4j-over-slf4j")) // Excluded to allow Cassandra to run embedded
```

### Use Case

We have the tree following tables in Cassandra storing information about students, subjects and their marks.
```
CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
CREATE TABLE my_keyspace.students(
    id text PRIMARY KEY, 
    first_name text, 
    last_name text
);,
CREATE TABLE my_keyspace.subjects(
    id text PRIMARY KEY, 
    subject_name text, 
    coefficient int
);
CREATE TABLE my_keyspace.marks(
    id text PRIMARY KEY,
    student_id text,
    subject_id text,
    mark float
);
```
 
We want to get this data from Cassandra, join them and do the following aggregations :
 * calculate the means by subject and student
 * calculate the general mean using the means by subject and student and the coefficient of each subject

### Example

We have in Cassandra this data

```
+---+----------+---------+
| id|first_name|last_name|
+---+----------+---------+
|  1|  Benjamin| Franklin|
|  2|    Thomas|Jefferson|
+---+----------+---------+

+---+-----------+------------+
| id|coefficient|subject_name|
+---+-----------+------------+
|  1|          9|        Math|
|  2|          3|      French|
+---+-----------+------------+

+---+----+----------+----------+
| id|mark|student_id|subject_id|
+---+----+----------+----------+
|  4|20.0|         1|         2|
|  3|18.0|         1|         2|
|  5|17.0|         2|         1|
|  1|13.5|         1|         1|
|  6|16.0|         2|         1|
|  2|12.5|         1|         1|
+---+----+----------+----------+
```

We want to have as result this DataFrame
```
+----------+-----------------------------------+------------+----------+---------+
|student_id|                      subject_means|general_mean|first_name|last_name|
+----------+-----------------------------------+------------+----------+---------+
|         1|[[Math, 9, 13.0],[French, 3, 19.0]]|        14.5|  Benjamin| Franklin|
|         2|                  [[Math, 9, 16.5]]|        16.5|    Thomas|Jefferson|
+----------+-----------------------------------+------------+----------+---------+

```

### Test

This project contains only one test using the embedded Cassandra and the function from spark-testing-base to compare 
the expected DataFrame with the result from the `joinTable()` function

To run the test just run
```
sbt compile
```
