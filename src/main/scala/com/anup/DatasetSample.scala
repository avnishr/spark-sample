package com.anup

import java.util.Properties

import com.anup.core.{ParseSQL, Pipeline}
import net.liftweb.json._
import org.apache.spark.sql.catalyst.expressions.{Literal, Nvl}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.transformers.{BaseTransfomer, DateTransformer, NullTransformer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.io.Source


object DatasetSample {


  def main(args: Array[String]): Unit = {


    loadPipelineConfig

    val spark : SparkSession  = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val prop = loadProperties()
    val pipeline = loadPipelineConfig()

    val df = spark
      .read
      .schema(readSchema(pipeline))
      .csv("file:///C:/tmp/data.csv")

    df.show()
    val tDF = transformDF(df, pipeline)

    tDF.write.mode(SaveMode.Overwrite).saveAsTable("F")
    tDF.show()


    val parser = new ParseSQL(spark)
    val tableList = parser.generateTableDependencies("sample.properties")

    val tables = spark.catalog.listTables()

    import spark.sqlContext.implicits._

    val catalogTables = spark.catalog.listTables().map( x => x.name).collect()

    val list = parser.flattenDependencies(tableList)


    Thread.sleep(2 * 60 * 1000)

  }

  def loadProperties(): Properties = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    val properties: Properties = new Properties()
    properties.load(inputStream)
    properties
  }

  def loadPipelineConfig() = {

    implicit val formats = net.liftweb.json.DefaultFormats

    val mappingURL = DatasetSample.getClass.getClassLoader.getResource("pipeline.json")
    val source = Source.fromURL(mappingURL)
    val jsonString = readAll(source)
    val jValue = parse(jsonString)
    val pipeline = jValue.extract[Pipeline]
    pipeline
  }

  def readSchema(pipeline : Pipeline) : StructType = {

    val fieldTypesTemp : Array[StructField] = new Array[StructField](pipeline.fields.length)

    for (column <- pipeline.fields) column.fieldType match {

      case "string" =>  fieldTypesTemp(column.ordinal) = StructField(column.name, StringType, true)
      case "int"    =>  fieldTypesTemp(column.ordinal) = StructField(column.name, IntegerType, true)
      case "bool"   =>  fieldTypesTemp(column.ordinal) = StructField(column.name, BooleanType, true)
      case _        => None
    }

    StructType(fieldTypesTemp.toSeq)
  }

  def transformDF( _df: DataFrame, pipeline :Pipeline ) : DataFrame = {
    val transformer  = Nil
    var df = _df
    val array = for (column <- pipeline.fields;  transformer <- column.transformers) yield transformer.name match {
      case "NullTransformer" => (column.name, new NullTransformer(transformer.params))
      case "DateTransformer" => (column.name, new DateTransformer(transformer.params))
    }

    for ( (column, tx ) <- array) {
      df = df.withColumn(column + "_mod", tx.transform(df.col(column)))
      df = df.drop(column).withColumnRenamed(column + "_mod", column)
    }
    df
  }

  def readAll(source: Source) = {
    val sb = new StringBuilder
    var cp = 0
    for (line <- source.getLines) {
      sb.append(line)
    }
    source.close()
    sb.toString()
  }

}