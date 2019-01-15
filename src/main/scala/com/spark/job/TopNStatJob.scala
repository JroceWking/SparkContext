package com.spark.job

import java.util.Date
import java.text.SimpleDateFormat
import caseclass.DayTop
import com.spark.SparkDAO.StatDao
import com.spark.model.{CityTop, DayVideoAccessStart, TrafficTop}
import com.spark.utils.DateUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object TopNStatJob {
  val sdf = new SimpleDateFormat("yyyy - MM - dd")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob").
      config("Spark.sql.sources.partitionColumnTypeInference.enabled","false").
      master("local[2]").getOrCreate()
    val accessDF = spark.read.format("parquet").
      load("file:///opt/outputlog")

      val day = "2017-05-11"
//    accessDF.printSchema()
//    accessDF.show(false)
    videoAccessTopNStart(spark,accessDF,day)
//
//      cityAccessTopNStart(spark,accessDF,day)
//    trafficAccessTopNStat(spark,accessDF,day)
    spark.stop()

  }


  def trafficAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day:String) = {
    import spark.implicits._
    val trafficTopDF = accessDF.filter($"day"  ===day).filter($"courseType" === "video").groupBy("day","courseId").agg(sum("traffic").as("times"))
    trafficTopDF.show()



    trafficTopDF.foreachPartition(partitionOfRecords => {
      val list = new ListBuffer[TrafficTop]



      partitionOfRecords.foreach(info => {
        import spark.implicits._
//        val day =sdf.format(info.getAs("day"))
       val day =sdf.format(info.getAs("day")).replace("-","").replace(" ","")
//       val day =  String.valueOf(daytwo)
//        val day = "20170511"
        val courseId =info.getAs[Long]("courseId")
        val times = info.getAs[Long]("times")

        list.append(TrafficTop(day,courseId,times))


      })
      StatDao.insertTraffic(list)

  })
  }

  def cityAccessTopNStart(spark: SparkSession, accessDF: DataFrame,day:String): Unit =  {

       import spark.implicits._
       var cityTopDF = accessDF.filter($"day"  ===day).filter($"courseType" === "video").groupBy("day","city","courseId").agg(count("courseId").as("times"))
//         cityTopDF.show()

    try{

   val top3 = cityTopDF.select(cityTopDF("day"),
      cityTopDF("courseId"),
      cityTopDF("city"),
      cityTopDF("times"),
      row_number().over(Window.partitionBy(cityTopDF("city")).orderBy(cityTopDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3").foreachPartition(partitionOfRecords => {
           val list = new ListBuffer[CityTop]

           partitionOfRecords.foreach(info => {
             val day =sdf.format(info.getAs("day")).replace("-","").replace(" ","")
             val courseId = info.getAs[Long]("courseId")
             val city =info.getAs[String]("city")
             val times = info.getAs[Long]("times")
             val times_rank = info.getAs[Int]("times_rank")

             list.append(CityTop(day,courseId,city,times,times_rank))


           })
           StatDao.insertCityTop(list)

         })



//      top3.foreachPartition(partitionOfRecords => {
//      val list = new ListBuffer[CityTop]
//
//      partitionOfRecords.foreach(info => {
//        val day =sdf.format(info.getAs("day")).replace("-","").replace(" ","")
//        val city =info.getAs[String]("city")
//        val courseId = info.getAs[Long]("courseId")
//        val times = info.getAs[Long]("times")
//        val times_rank = info.getAs("times_rank")
//
//        list.append(CityTop(day,city,courseId,times,times_rank))
//
//
//      })
//      StatDao.insertCityTop(list)
//
//    })
    } catch {
      case e: Exception => e.printStackTrace()
    }
//


  }



  def videoAccessTopNStart(spark: SparkSession, accessDF:DataFrame,day:String): Unit ={
    import spark.implicits._
    val dayTopDF = accessDF.filter($"day" === day ).filter($"courseType" === "video")
      .groupBy("day", "courseId").agg(count("courseId").as("times")).orderBy($"times".desc)
    dayTopDF.show()



//
//   accessDF.createOrReplaceTempView("access_log")
//    val dayTopDF = spark.sql("select day,courseId,count(1) as times" +
//      " from access_log where day = '2017-05-11' and courseType = 'video' group by day,courseId order by times desc ")
//
//    dayTopDF.sqlContext.sql("desc formatted access_log").show()

//
//    import spark.implicits._
//    val dayTopDF = accessDF.filter($"day" === "2017-05-11" && $"courseType" === "video").groupBy("day","courseId")
//      .groupBy("day", "courseId")
//      .agg(count(1).as("times")).orderBy($"times".desc)
//    dayTopDF.show()

    try
      dayTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayTop]

        partitionOfRecords.foreach(info => {
          val day =sdf.format(info.getAs("day")).replace("-","").replace(" ","")

          val courseId = info.getAs[Long]("courseId")
          val times = info.getAs[Long]("times")
          list.append(DayTop(day, courseId, times))


        })
        StatDao.insertDayTop(list)

      })

    catch {
      case e: Exception => e.printStackTrace()
    }


  }


}



