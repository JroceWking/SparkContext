package com.spark.job
import com.spark.utils.AccessConverUtil
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import utils.ConvertUtil
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
      val accessRDD  = spark.sparkContext.textFile("file:///opt/format.log")
         spark
//      accessRDD.take(10).foreach(println)
//    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConverUtil.parseLog(x)),AccessConverUtil.struct)
 //     val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConverUtil.parseLog(x)),AccessConverUtil.struct)
//     accessRDD.take(3).foreach(println)

      val accessDF = spark.createDataFrame(accessRDD.map(x => ConvertUtil.parseLong(x)),AccessConverUtil.struct)
  println(accessDF)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("file:///opt/outputlog")
             spark.stop()


  }
}
