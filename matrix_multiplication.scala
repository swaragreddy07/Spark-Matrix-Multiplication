// Databricks notebook source
// MAGIC %md
// MAGIC ## Matrix Multiplicaiton of Coordinate Matrices

// COMMAND ----------

// Define two small matrices M and N
val M = Array(Array(1.0, 2.0), Array(3.0, 4.0))
val N = Array(Array(5.0, 6.0), Array(7.0, 8.0))

// COMMAND ----------

//// Convert the small matrices to RDDs
val M_RDD_Small = sc.parallelize(M.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })
val N_RDD_Small = sc.parallelize(N.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })

// COMMAND ----------

// MAGIC %md
// MAGIC #### [TODO] Function to perform matrix multiplication of RDDs (100 Points)

// COMMAND ----------

def COOMatrixMultiply ( M: RDD[((Int,Int),Double)], N: RDD[((Int,Int),Double)] ): RDD[((Int,Int),Double)] = {
  M.map{case (index, value) => (index._2, (index._1, value))}
   .join(N.map{case (index,value) => (index._1, (index._2,value))})
   .map{case (key, ((i, value_1), (j, value_2))) => ((i, j), value_1 * value_2)}
   .reduceByKey(_+_)
}

// COMMAND ----------

val R_RDD_Small = COOMatrixMultiply(M_RDD_Small, N_RDD_Small)
R_RDD_Small.collect.foreach(println)
