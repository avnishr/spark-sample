package com.anup.core

import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class TableDependencies ( name : String, dependencies: Array[String])

/**
 * This class is used to parse the SQL statements in the config file and order the SQL(s) so that
 * the dependencies are created first before the parent tables are created.
 *
 * The advantage of this class is that it allows the user to provide sql to be executed by Spark framework
 * in any manner the user wants. The class reorders them
 * @param spark
 */
class ParseSQL ( spark : SparkSession) {

  /**
   * Determine the dependant tables for the query
   * */
  def getTablesForQuery(sqlQuery : String) : Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(sqlQuery)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
  }

  /**
   * Read SQL queries from the configuration file.
   * Arrange the table and their queries into properties object
   * @param configFile  The configuration file you would read SQL files from
   * @return
   */
  def getSQLFromConfig(configFile : String) : Properties = {
    val sqlQueries = new Properties
    sqlQueries.load(this.getClass.getClassLoader.getResourceAsStream(configFile))
    sqlQueries
  }

  /**
   * This part of the code is responsible for read the configuration file and generating the
   * Map of tables and its SQL
   * @param configFile SQL properties file
   * @return
   */
  def generateTableDependencies(configFile:String) : mutable.Map[String, List[String]] = {
    val sqlProps = getSQLFromConfig(configFile)
    val tableHashMap : mutable.Map[String, List[String]] = mutable.HashMap.empty[String, List[String]]

    for ( table <- sqlProps.keySet().toArray) {
      val dependenciesForTable = getTablesForQuery(sqlProps.getProperty(table.toString))
      tableHashMap += (table.toString -> dependenciesForTable.toList)
    }
    tableHashMap
  }

  /**
   * Generates a dependency graph which is repsonsible for executing the statements in an order that
   * all the dependent tables are created first before the other tables are created.
   *
   * @param dependencies  HashMap of table and dependencies
   * @return
   */
  def flattenDependencies(dependencies : mutable.Map[String, List[String]]) : List[String] = {

    /* Let us create a hash map of tables and their dependencies
    The hash Map will help in faster lookups.

    We continue to use lists as list preserve the order of pushing the elements
    finalList will be the final list returned
     */

    val finalList: ListBuffer[String] = new ListBuffer[String]()
    finalList.prepend(dependencies.keys.toList:_*)
    val wipStack : mutable.Stack[String] = new mutable.Stack[String]()

    for ( table  <- dependencies.keys ) {
      val depTables = dependencies.get(table).get
      wipStack.pushAll(depTables)
      val tempList : ListBuffer[String] = new ListBuffer[String]()
      while ( wipStack.length > 0) {
        // create a temporary list to store the dependencies for this level
        val itemsInStack = wipStack.length

        val table = wipStack.pop()
        finalList.append(table)
        dependencies.get(table) match {
          case x : Some[List[String]] => tempList.append(x.get:_*)
          case _ => None
        }

        if (itemsInStack == 1) {
          wipStack.pushAll(tempList)
          tempList.clear()
        }
      }
    }
    val reverseList = finalList.reverse
    val tableSet = new mutable.LinkedHashSet[String]()
    for ( name <- reverseList) {
      tableSet.add(name)
    }
    tableSet.toList
  }
}
