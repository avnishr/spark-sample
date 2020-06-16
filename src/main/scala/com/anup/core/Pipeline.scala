package com.anup.core

case class KeyValue(val key:String, val value:String)

case class GenericTransformer(val name: String, val params : Array[KeyValue])

case class Columns ( val ordinal : Int, val name : String, val fieldType: String, val transformers : Array[GenericTransformer])

case class Pipeline (val fields: Array[Columns])