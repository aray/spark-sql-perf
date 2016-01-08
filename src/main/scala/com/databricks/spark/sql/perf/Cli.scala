package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by andrew on 1/7/16.
  */
object Cli {

  def usage() = {
    println(
      """spark-sql-perf cli usage
        |Generate tables: Cli generate <master> <table-path> <format> <scale-factor> <tpc-ds-tools>
        |Run query set:   Cli run <master> <table-path> <format>
      """.stripMargin)
    sys.exit(42)
  }

  def main(args: Array[String]) {
    args match {
      case Array("generate", master, tablePath, format, scaleFactor, tpcdsKitTools) =>
        val sc = new SparkContext(master, "spark-sql-perf generate")
        val sqlContext = new SQLContext(sc)
        val tables = new Tables(sqlContext, tpcdsKitTools, scaleFactor.toInt)
        tables.genData(tablePath, format, false, true, false, false, false)
      case Array("run", master, tablePath, format) =>
        val sc = new SparkContext(master, "spark-sql-perf run")
        val sqlContext = new SQLContext(sc)
        val tables = new Tables(sqlContext, null, 0) //dsdgen dir and scale factor are not needed
        tables.createTemporaryTables(tablePath, format)
        val tpcds = new TPCDS(sqlContext) // defaults to results in /spark/sql/performance/*/
        val experiment = tpcds.runExperiment(tpcds.impalaKitQueries, verbose = true)
        experiment.waitForFinish(Integer.MAX_VALUE)
      case _ => usage()
    }
  }
}
