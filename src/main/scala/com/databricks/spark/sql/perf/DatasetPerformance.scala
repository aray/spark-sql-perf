package com.databricks.spark.sql.perf

import org.apache.spark.sql.expressions.Aggregator

case class Data(id: Long)

case class SumAndCount(var sum: Long, var count: Int)

trait DatasetPerformance extends Benchmark {

  import sqlContext.implicits._

  val numLongs = 100000000
  val ds = sqlContext.range(1, numLongs)
  val rdd = sparkContext.range(1, numLongs)

  val smallNumLongs = 1000000
  val smallds = sqlContext.range(1, smallNumLongs)
  val smallrdd = sparkContext.range(1, smallNumLongs)

  def allBenchmarks =  range ++ backToBackFilters ++ backToBackMaps ++ computeAverage

  val range = Seq(
    new Query(
      "DS: range",
      ds.as[Data].toDF(),
      executionMode = ExecutionMode.ForeachResults),
    new Query(
      "DF: range",
      ds.toDF(),
      executionMode = ExecutionMode.ForeachResults),
    RDDCount(
      "RDD: range",
      rdd.map(Data))
  )

  val backToBackFilters = Seq(
    new Query(
      "DS: back-to-back filters",
      ds.as[Data]
        .filter(_.id % 100 != 0)
        .filter(_.id % 101 != 0)
        .filter(_.id % 102 != 0)
        .filter(_.id % 103 != 0).toDF()),
    new Query(
      "DF: back-to-back filters",
      ds.toDF()
        .filter("id % 100 != 0")
        .filter("id % 101 != 0")
        .filter("id % 102 != 0")
        .filter("id % 103 != 0")),
    RDDCount(
      "RDD: back-to-back filters",
      rdd.map(Data)
        .filter(_.id % 100 != 0)
        .filter(_.id % 101 != 0)
        .filter(_.id % 102 != 0)
        .filter(_.id % 103 != 0))
  )

  val backToBackMaps = Seq(
    new Query(
      "DS: back-to-back maps",
      ds.as[Data]
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L)).toDF()),
    new Query(
      "DF: back-to-back maps",
      ds.toDF()
        .select($"id" + 1 as 'id)
        .select($"id" + 1 as 'id)
        .select($"id" + 1 as 'id)
        .select($"id" + 1 as 'id)),
    RDDCount(
      "RDD: back-to-back maps",
      rdd.map(Data)
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L))
        .map(d => Data(d.id + 1L)))
  )

  val average = new Aggregator[Long, SumAndCount, Double] {
    override def zero: SumAndCount = SumAndCount(0, 0)

    override def reduce(b: SumAndCount, a: Long): SumAndCount = {
      b.count += 1
      b.sum += a
      b
    }

    override def finish(reduction: SumAndCount): Double = reduction.sum.toDouble / reduction.count

    override def merge(b1: SumAndCount, b2: SumAndCount): SumAndCount = {
      b1.count += b2.count
      b1.sum += b2.sum
      b1
    }
  }.toColumn

  val computeAverage = Seq(
    new Query(
      "DS: average",
      smallds.as[Long].select(average).toDF(),
      executionMode = ExecutionMode.CollectResults),
    new Query(
      "DF: average",
      smallds.toDF().selectExpr("avg(id)"),
      executionMode = ExecutionMode.CollectResults),
    new SparkPerfExecution(
      "RDD: average",
      Map.empty,
      prepare = () => Unit,
      run = () => {
        val sumAndCount =
          smallrdd.map(i => (i, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        sumAndCount._1.toDouble / sumAndCount._2
      })
  )
}
