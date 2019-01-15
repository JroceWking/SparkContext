package com.spark.job
import com.spark.utils.DateUtils
import org.apache.spark.sql.SparkSession
object SparkStatFormatjob {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val access = spark.sparkContext.textFile("file:///opt/init.log")
    //   val st =  access.take(10).foreach(println)
    //      access.take(10).foreach(println)
    val Save = access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(10).replaceAll("\"", " ")
      val traffic = splits(9)


            DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
//      url
//    }).take(10).foreach(println)
    }).saveAsTextFile("file:///opt/out7")
    spark.stop()
    //    val su = st.toString.getBytes("utf-8")
    //    su.foreach(println)
  }
}
