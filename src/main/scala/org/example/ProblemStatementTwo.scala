package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col}

object ProblemStatementTwo {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
                 .appName("ProblemStatementTwo")
                 .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val filePath = spark.sparkContext.getConf.get("spark.app.input.path")

      val avroFileDf = spark.read.format("avro").load(filePath)

      val FilterDataDf = avroFileDf.select("*").where(col("pop.campaign_ref").isNotNull && !col("pop.campaign_ref").equalTo(""))

      val lookupSeq =  Seq(("JCDECAUX_FR", true), ("JCDECAUX_US", true), ("JCDECAUX_GB", false))
      val lookupDf  =  spark.createDataFrame(lookupSeq).toDF("market","isComplianceSupportedMarket")
          lookupDf.show(false)

      val joinCondition = FilterDataDf("header.market") === lookupDf("market")

      val joinType = "left"

      val joinDf = FilterDataDf.join(broadcast(lookupDf), joinCondition, joinType)
                   .where(col("market").isNotNull)
                   .filter(col("isComplianceSupportedMarket").equalTo(true))

          joinDf.show(5,false)

      joinDf.write.partitionBy("market" ).mode("overwrite").format("parquet").save("C:\\Users\\yogesh.ahire\\Desktop\\test\\output\\AvroToParquet")

  }
}

