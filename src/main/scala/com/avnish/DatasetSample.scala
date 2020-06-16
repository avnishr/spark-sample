package com.avnish

import java.util.Properties

import org.apache.spark.sql.catalyst.expressions.{Literal, Nvl}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.io.Source


object DatasetSample {


  def main(args: Array[String]): Unit = {

    val spark : SparkSession  = SparkSession.builder()
      .appName("test")
      .getOrCreate()

    val tables = spark.catalog.listTables()

    import spark.sqlContext.implicits._

    spark.catalog.listTables().map( x => x.name).foreach(x => println(x))

  }


}