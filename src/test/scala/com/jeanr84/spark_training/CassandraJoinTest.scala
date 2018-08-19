package com.jeanr84.spark_training

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CassandraJoinTest extends FunSuite with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate with DataFrameSuiteBase {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Because sc is overridden by two trait (SharedSparkContext and SparkTemplate), so we override it with SparkTemplate.sc because it contains Cassandra configs
  override def sc: SparkContext = SparkTemplate.sc

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    createTables()
  }

  def executeCassandraCommands(cassandraCommands: List[String]): Unit =
    connector.withSessionDo { session =>
      cassandraCommands.foreach(session.execute)
    }

  def createTables(): Unit = {
    val cassandraCommands = List(
      "CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};",
      "CREATE TABLE my_keyspace.students(" +
        "id text PRIMARY KEY, " +
        "first_name text, " +
        "last_name text" +
        ");",
      "CREATE TABLE my_keyspace.subjects(" +
        "id text PRIMARY KEY, " +
        "subject_name text, " +
        "coefficient int" +
        ");",
      "CREATE TABLE my_keyspace.marks(" +
        "id text PRIMARY KEY, " +
        "student_id text, " +
        "subject_id text," +
        "mark float" +
        ");"
    )
    executeCassandraCommands(cassandraCommands)
  }

  def initTables(): Unit = {
    val cassandraCommands = List(
      "INSERT INTO my_keyspace.students(id, first_name, last_name) VALUES ('1', 'Benjamin', 'Franklin');",
      "INSERT INTO my_keyspace.students(id, first_name, last_name) VALUES ('2', 'Thomas', 'Jefferson');",
      "INSERT INTO my_keyspace.subjects(id, subject_name, coefficient) VALUES ('1', 'Math', 9);",
      "INSERT INTO my_keyspace.subjects(id, subject_name, coefficient) VALUES ('2', 'French', 3);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('1', '1', '1', 13.5);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('2', '1', '1', 12.5);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('3', '1', '2', 18);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('4', '1', '2', 20);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('5', '2', '1', 17);",
      "INSERT INTO my_keyspace.marks(id, student_id, subject_id, mark) VALUES ('6', '2', '1', 16);"
    )
    executeCassandraCommands(cassandraCommands)
  }

  test("Simple test case") {
    implicit val spark: SparkSession = sparkSession

    initTables()

    val result = CassandraJoin.joinTables()
    val expectedData = Seq(
      Row("1", Array(Row("Math", 9, 13.0), Row("French", 3, 19.0)), 14.5, "Benjamin", "Franklin"),
      Row("2", Array(Row("Math", 9, 16.5)), 16.5, "Thomas", "Jefferson")
    )

    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      result.schema
    )

    assertDataFrameEquals(expected, result)
  }
}