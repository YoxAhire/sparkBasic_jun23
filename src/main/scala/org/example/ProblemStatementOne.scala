package org.example

import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{SparkSession}

object ProblemStatementOne {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
                  .appName("ProblemStatementOne")
                  .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val filePath = spark.sparkContext.getConf.get("spark.app.input.path")

      val parquetFile = spark.read.parquet(filePath)

      val filterRecordModelType  =  parquetFile.select("*").where("(modeltype) not in ('static','Static','STATIC')")

      val newColAdd = filterRecordModelType.withColumn("newroutetowncode", when(col("routetowncode").isNull,"NONE").otherwise(col("routetowncode")))

      val myUdf: UserDefinedFunction = udf((medialength: Int) => {
                if (medialength == 0 || medialength == null) 6
                else medialength
      })

      val derivedData = newColAdd.withColumn("newmedialength", myUdf(col("medialength")))

      val finalRecord = derivedData.select(col("routeframecode"),col("businessareacode"),col("businessareagroupcode"),col("businessareaname"),col("channelname"),col("digital"))

      finalRecord.write.cassandraFormat("frame_by_routeframecode", "asset").mode("append").save()

      finalRecord.show(5)

  }
}
