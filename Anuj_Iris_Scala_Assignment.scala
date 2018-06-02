// Databricks notebook source
sc

// COMMAND ----------

val scalaIrisRdd = sc.textFile("/FileStore/tables/iris_data-04fff.txt")

// COMMAND ----------

scalaIrisRdd.count()

// COMMAND ----------

scalaIrisRdd.getNumPartitions

// COMMAND ----------

scalaIrisRdd.glom().collect()

// COMMAND ----------

val scala_lines_setosa = scalaIrisRdd.filter(line => line.contains ("setosa"))

// COMMAND ----------

scala_lines_setosa.collect()

// COMMAND ----------

val scala_lines_versicolor = scalaIrisRdd.filter(line => line.contains ("versicolor"))

// COMMAND ----------

scala_lines_versicolor.collect()

// COMMAND ----------

val scala_lines_virginica = scalaIrisRdd.filter(line => line.contains ("virginica"))

// COMMAND ----------

scala_lines_virginica.collect()

// COMMAND ----------

scala_lines_virginica.count()

// COMMAND ----------


