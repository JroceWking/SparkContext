package com.spark.SparkDAO
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer
import java.util._

import caseclass.DayTop
import org.apache.spark._
import com.spark.model.{CityTop, DayVideoAccessStart, TrafficTop}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.MySQLUtil

import scala.collection.mutable.ListBuffer

object StatDao {

  def insertDayTop(list: ListBuffer[DayTop]) = {
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day,courseId,times) values(?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1,ele.day)
        state.setLong(2, ele.courseId)
        state.setLong(3, ele.times)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }


//   def insertCityTop(list: ListBuffer[CityTop]): Unit = {
//      try {
//
//        var connect:Connection = null
//        var Scity:PreparedStatement = null
//
//        connect = MySQLUtil.getConnection()
//        connect.setAutoCommit(false)
//        val sql = "insert into day_city_access_topn_stat(day,city,courseId,times) values(?,?,?)"
//        Scity = connect.prepareStatement(sql)
//        for (ee <- list) {
//          Scity.setString(1, ee.day)
//          Scity.setString(1, ee.city)
//          Scity.setLong(2, ee.courseId)
//          Scity.setLong(3, ee.times)
//          Scity.addBatch()
//
//        }
//        Scity.executeBatch()
//        connect.commit()
//
//      }catch {
//
//       case e:Exception => e.printStackTrace()
//     }finally {
//          MySQLUtil.release(connect,Scity)
//      }
//
//
//   }



  def insertCityTop(list: ListBuffer[CityTop]) = {
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into day_city_access_topn_stat_one_1(day,courseId,city,times,times_rank) values(?,?,?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1,ele.day)
        state.setLong(2, ele.courseId)
        state.setString(3, ele.city)
        state.setLong(4, ele.times)
        state.setInt(5, ele.times_rank)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }

  def insertTraffic(list:ListBuffer[TrafficTop]): Unit ={
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into day_traffic_city_access_topn_stat_one(day,courseId,traffic) values(?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1,ele.day)
        state.setLong(2, ele.courseId)
        state.setLong(3, ele.times)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }


  def  delectTable(day:String): Unit ={
    val tables = Array("day_top", "city_top", "traffic_top")
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      for(table <- tables) {
        val deleteSQL = s"delete from $table where day=?"
        val state = con.prepareStatement(deleteSQL)
        state.setString(1, day)
        state.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }
}
