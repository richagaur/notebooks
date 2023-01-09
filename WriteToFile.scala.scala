// Databricks notebook source
val cosmosEndpoint = <cosmos-endpoint>
val cosmosMasterKey = <cosmos-master-key>
val cosmosDatabaseName = <cosmos-database-name>
val cosmosContainerName = <cosmos-container-name>

val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
)


// COMMAND ----------

//batch example

val filePath = "/tmp/test.csv"
val batchSize = 10000
var df = spark.read.format("cosmos.oltp").options(cfg).load().limit(batchSize)

while (!df.rdd.isEmpty()){
  df.coalesce(1).write.format("csv").mode("append").option("header", "true").save(filePath)
  df = spark.read.format("cosmos.oltp").options(cfg).load().limit(batchSize)
}

// COMMAND ----------

//streaming example 


val checkpointLocation = "/tmp/cp"
val filePath = "/tmp/streaming1.csv"

val changeFeedCfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName,
  "spark.cosmos.read.inferSchema.enabled" -> "true",   
  "spark.cosmos.changeFeed.startFrom" -> "Beginning",
  "spark.cosmos.changeFeed.mode" -> "Incremental",
  "spark.cosmos.changeFeed.itemCountPerTriggerHint" -> "100000"
)

val writeCfg = Map("path" -> filePath,
  "checkpointLocation" -> checkpointLocation,
  "header" -> "true"
)

val changeFeedDF = spark.readStream.format("cosmos.oltp.changeFeed")
      .options(changeFeedCfg)  
      .load()

changeFeedDF
  .coalesce(1)
  .writeStream
  .format("csv")
  .options(writeCfg)
  .start()
  .awaitTermination()
