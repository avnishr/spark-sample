package org.apache.spark.sql.transformers

import com.anup.core.KeyValue
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, FromUnixTime, Literal, UnixTimestamp}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class DateTransformer (params: Array[KeyValue]) extends BaseTransfomer {

  def getSourceDateFormat(): Try[String] = {

    var sourceDateFmt: Option[String] = None

    for ( param <- params if  sourceDateFmt == None) {
      sourceDateFmt =  param.key match {
        case ("sourceDateFormat") => Some(param.value)
        case _ => None
      }
    }

    Try(sourceDateFmt getOrElse "Missing parameter sourceDateFormat")
  }

  def getTargetDateFormat() : Try[String] = {
    var targetDateFormat: Option[String] = None

    for ( param <- params if  targetDateFormat == None) {
      targetDateFormat =  param.key match {
        case ("targetDateFormat") => Some(param.value)
        case _ => None
      }
    }

    Try(targetDateFormat getOrElse  "Missing parameter targetDateFormat")
  }

  def validate(): Option[List[Exception]] = {

    val errors : ListBuffer[Exception] = new ListBuffer[Exception]()
    getTargetDateFormat match {
      case ex : Exception => errors.append(ex)
      case _   =>  None
    }

    getSourceDateFormat match {
      case ex : Exception => errors.append(ex)
      case _   =>  None
    }

    errors match {
      case list : ListBuffer[Exception] => Some(errors.toList)
      case _  => None
    }

  }

  override def transform(col : Column) : Column = {
      Column(FromUnixTime(UnixTimestamp(col.expr, lit(this.getSourceDateFormat().get).expr), lit(this.getTargetDateFormat().get).expr))
  }

}