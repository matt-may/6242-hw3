package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object MyFunctions {
  def addInt( a:Int, b:Int, c:Int ) : Int = {
      var sum:Int = 0
      sum = a + b

      return sum
   }
}

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    // Instructions:
    // Assume that 80% of the edge weight comes from the source node and 20%
    // from the target node. When loading the edges, parse the edge weights
    // using the toInt method and before cascading, filter out (ignore) all
    // edges whose edge weights equal 1. That is, only consider edges whose
    // edge weights do not equal 1.

    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))

    // Parse the file, converting the strings to integers.
    val tsv = file.map(_.split("\t").map(_.toInt))

    // Filter to exclude edges with a weight = 1.
    val filtered = tsv.filter(arr => arr(2) != 1)

    // Prepare (source node, edge weight) key pairs.
    val srcs = filtered.map(arr => (arr(0), 0.8 * arr(2)))

    // Prepare (target node, edge weight) key pairs.
    val tgts = filtered.map(arr => (arr(1), 0.2 * arr(2)))

    // Prepare a union of the two sets of key pairs.
    val union = srcs.union(tgts)

    // Reduce, summing.
    val reduced = union.reduceByKey(_ + _)

    // Instructions:
    // You need to format the result before saving to file (Tip: use map and
    // mkString). The result doesn’t need to be sorted.

    // Prepare for writing.
    val counts = reduced.map { case (key, value) => s"$key\t$value"}

    // store output on given HDFS path.
    counts.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
