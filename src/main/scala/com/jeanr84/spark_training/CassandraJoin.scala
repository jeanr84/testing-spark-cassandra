package com.jeanr84.spark_training

import java.sql.Struct

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, collect_list, struct, udf}

import scala.collection.mutable


object CassandraJoin {

  // We'll run our project in local mode
  val sparkConf: SparkConf = new SparkConf().
    setAppName("cassandra-join").
    setMaster("local[*]")

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()


  def readFromCassandraTable(sparkSession: SparkSession, keyspace: String, table: String): DataFrame =
    sparkSession
      .read
      .cassandraFormat(table, keyspace)
      .load()

  val general_mean: UserDefinedFunction = udf {
    s: mutable.WrappedArray[Row] => {
      val sumCoeff = s.map(struct => struct.getInt(struct.fieldIndex("coefficient"))).sum
      val multiply: Double = s.map(struct => struct.getInt(struct.fieldIndex("coefficient"))
        * struct.getDouble(struct.fieldIndex("avg(mark)"))).sum
      multiply / sumCoeff
    }
  }

  def joinTables(): DataFrame = {
    val students = readFromCassandraTable(sparkSession, "my_keyspace", "students")
    val subjects = readFromCassandraTable(sparkSession, "my_keyspace", "subjects")
    val marks = readFromCassandraTable(sparkSession, "my_keyspace", "marks")

    marks
      .groupBy("student_id", "subject_id")
      .mean("mark")
      .join(subjects.withColumnRenamed("id", "subject_id"), Seq("subject_id"))
      .withColumn("mean", struct("subject_name", "coefficient", "avg(mark)"))
      .groupBy("student_id")
      .agg(
        collect_list("mean").as("subject_means")
      )
      .withColumn("general_mean", general_mean(col("subject_means")))
      .join(students.withColumnRenamed("id", "student_id"), Seq("student_id"))
  }
}
