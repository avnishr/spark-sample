package org.apache.spark.sql.transformers

import org.apache.spark.sql.{Column, DataFrame}

abstract class BaseTransfomer {

  def transform(col : Column) : Column

}
