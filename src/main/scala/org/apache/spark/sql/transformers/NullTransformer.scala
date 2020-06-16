package org.apache.spark.sql.transformers

import com.anup.core.KeyValue
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression, Literal, Nvl}

class NullTransformer(params: Array[KeyValue])  extends  BaseTransfomer {

  def getNullValue(): String = {
    params(0).value
  }

  override def transform(col: Column): Column = {
    Column(Coalesce(Seq(col.expr, Literal(getNullValue()))))
  }
}
