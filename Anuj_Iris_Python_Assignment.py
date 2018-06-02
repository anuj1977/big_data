# Databricks notebook source
# Entry point in spark,helps driver prog to connect to cluster 
sc

# COMMAND ----------

spark

# COMMAND ----------

spark_iris_rdd = sc.textFile("/FileStore/tables/iris_data-04fff.txt",3)

# COMMAND ----------

#data printed below is the result
spark_iris_rdd.collect()

# COMMAND ----------

spark_iris_rdd.count()

# COMMAND ----------

# filter is a transformation, will give another rdd
spark_lines_setosa = spark_iris_rdd.filter(lambda line : "setosa" in line)

# COMMAND ----------

spark_lines_setosa.collect()

# COMMAND ----------

spark_lines_setosa.count()

# COMMAND ----------

type(spark_lines_setosa)

# COMMAND ----------

type(spark_iris_rdd)

# COMMAND ----------

# filter is a transformation, will give another rdd
scala_lines_versicolor = spark_iris_rdd.filter(lambda line : "versicolor" in line)

# COMMAND ----------

scala_lines_versicolor.collect()

# COMMAND ----------

scala_lines_versicolor.count()

# COMMAND ----------

spark_lines_virginica = spark_iris_rdd.filter(lambda line : "virginica" in line)

# COMMAND ----------

spark_lines_virginica.collect()

# COMMAND ----------

spark_lines_virginica.count()

# COMMAND ----------

spark_iris_rdd.first()

# COMMAND ----------

spark_iris_rdd.getNumPartitions()

# COMMAND ----------

spark_iris_rdd.glom().collect()

# COMMAND ----------

map_words = spark_iris_rdd.map(lambda line : line.split(","))

# COMMAND ----------

map_words.collect()

# COMMAND ----------

flatmap_rdd = spark_iris_rdd.flatMap(lambda line : line.split(","))

# COMMAND ----------

#flatten the data
flatmap_rdd.collect()

# COMMAND ----------

flatmap_rdd.count()

# COMMAND ----------

word_with_one = flatmap_rdd.map(lambda word : (word,1))

# COMMAND ----------

word_with_one.collect()

# COMMAND ----------

word_count = word_with_one.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

word_count.collect()

# COMMAND ----------

word_count.count()

# COMMAND ----------


