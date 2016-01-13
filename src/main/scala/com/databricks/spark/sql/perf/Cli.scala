package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by andrew on 1/7/16.
  */
object Cli {

  def usage() = {
    println(
      """spark-sql-perf cli usage
        |Generate tables: Cli generate <table-path> <format> <scale-factor> <tpc-ds-tools>
        |Run query set:   Cli run <table-path> <format>
      """.stripMargin)
    sys.exit(42)
  }

  def getSqlCtx(appName: String): SQLContext = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }

  def main(args: Array[String]) {
    args match {
      case Array("generate", tablePath, format, scaleFactor, tpcdsKitTools) =>
        val sqlContext = getSqlCtx("spark-sql-perf generate")
        val tables = new Tables(sqlContext, tpcdsKitTools, scaleFactor.toInt)
        tables.genData(tablePath, format, true, false, false, false, false)
      case Array("run", tablePath, format) =>
        val sqlContext = getSqlCtx("spark-sql-perf run")
        val tables = new Tables(sqlContext, null, 0) //dsdgen dir and scale factor are not needed
        tables.createTemporaryTables(tablePath, format)
        val tpcds = new TPCDS(sqlContext) // defaults to results in /spark/sql/performance/*/
        val experiment = tpcds.runExperiment(tpcds.impalaKitQueries, verbose = true)
        experiment.waitForFinish(Integer.MAX_VALUE)
      case _ => usage()
    }
  }
}
