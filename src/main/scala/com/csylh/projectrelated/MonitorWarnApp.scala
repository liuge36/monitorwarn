package com.csylh.projectrelated

import java.util
import java.util.List

import com.csylh.utils.{OnlineAnalysisBroadcastAlertAppUtil, OnlineAnalysisInfluxDB2JavaUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import scalikejdbc.config.DBs

/**
  * Description: 监控预警平台
  *
  * * 1.消费kafka json数据转换为DF
  * *  重点参考：https://blog.csdn.net/shirukai/article/details/85211951
  * * 2.SQL逻辑
  * * 3.写入到InfluxDB
  * * 4.广播变量+更新(自定义预警关键词)
  *
  * @Author: 留歌36
  * @Date: 2019/12/17 10:35
  */
object MonitorWarnApp {
  def main(args: Array[String]): Unit = {
    //      val conf = new SparkConf().setMaster("local[2]").setAppName("")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("OffsetManageApp")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, String](
      "metadata.broker.list"->"192.168.1.200:9092",
      "group.id"->"liuge.group.id" ,// 消费的时候是以组为单位进行消费
      "auto.offset.reset" -> "smallest" // 从头消费
    )

    val topics = "onlinelogs2".split(",").toSet

    var bcAlertList:Broadcast[List[String]] = null
    var sqlstr:String = ""


    // 步骤一： 先获取offset
    import scalikejdbc._
    DBs.setup()
    val fromOffsets = DB.readOnly {
        implicit session => {
          SQL("select * from offset_storage  ").map(rs =>
            (TopicAndPartition(rs.string("topic"), rs.int("partitions")),rs.long("offset"))
          ).list().apply()
        }
    }.toMap

    for (ele <- fromOffsets){
      println("读取MySQL偏移量相关数据==>topic: " + ele._1.topic + ":" + ele._1.partition +":"+ele._2)
    }


    val stream = if (fromOffsets.isEmpty) { // 第一次
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    }else{ // 非第一次

      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
    }


    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()){
        println("~~~~~~~~~~~~~~~~~~~~~~~华丽的分割线~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println(s"留歌本轮的统计结果：${rdd.count()}条数据")

        try{
          import spark.implicits._
          val jsonDS =  rdd.map(_._2).toDS
          val allDF:DataFrame = spark.read.json(jsonDS)
//          allDF.show()

          // TODO...
          // 该批次处理的所有数据
          allDF.createOrReplaceTempView("cdhrolelogs")

          //===============开始调用自定义alert广播变量================================
          //        var bcAlertList:Broadcast[List[String]] = null
          bcAlertList = OnlineAnalysisBroadcastAlertAppUtil.getInstance().updateAndGet(spark, bcAlertList)
          val alertInfoList = bcAlertList.value
          if (!alertInfoList.isEmpty){
            // MySQL中的预警词频有数据

            // 定义 alertsql
            var alertsql = ""
            println("打印~~留歌自定义预警词频: print custom alert words:")

            import scala.collection.JavaConversions._
            for (alertInfo <- alertInfoList) {
              System.out.println(alertInfo)
              alertsql = alertsql + " logInfo like '%" + alertInfo + "%' or"
            }

            alertsql = alertsql.substring(0, alertsql.length - 2)

            // 定义sql
          sqlstr =
              "SELECT hostName,serviceName,logType,COUNT(logType) FROM cdhrolelogs GROUP BY hostName,serviceName,logType " +
              "union all " +
              "SELECT t.hostName,t.serviceName,t.logType,COUNT(t.logType) FROM " +
              "(SELECT hostName,serviceName,'alert' as logType FROM cdhrolelogs where " + alertsql + ") t GROUP BY t.hostName,t.serviceName,t.logType"

          }else{
            // MySQL中的预警词频为空表
            sqlstr =
              "SELECT hostName,serviceName,logType,COUNT(logType) " +
              "FROM cdhrolelogs  " +
              "GROUP BY hostName,serviceName,logType"
          }
          //===============结束调用自定义alert广播变量================================


          // ==========
//          val sqlstr = "SELECT hostName,serviceName,logType,COUNT(logType) " +
//            "FROM cdhrolelogs  " +
//            "GROUP BY hostName,serviceName,logType"

          val logtypecount: util.List[Row] = spark.sql(sqlstr).collectAsList

          var value = ""
          var host_service_logtype = ""
          import scala.collection.JavaConversions._
          for (rowlog <- logtypecount) {
            host_service_logtype = rowlog.get(0) + "_" + rowlog.get(1) + "_" + rowlog.get(2)
            value = value + "logtype_count,host_service_logtype=" + host_service_logtype + " count=" + String.valueOf(rowlog.getLong(3)) + "\n"
          }

          if (value.length > 0){
//            println("之前" +value)
            value = value.substring(0,value.length)
            println("之后" +value)
            OnlineAnalysisInfluxDB2JavaUtil.insertInfluDB(value)
          }


        }catch {
          case e:Exception => e.printStackTrace()

        }

      }

      // 将Offset 提交到外部存储保存  <==   步骤二：保存offset
      // ======================保存offset的模板 开始==================================
      val  offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        if (o.fromOffset != o.untilOffset) {
          println(s"消费主题 ${o.topic} 分区 ${o.partition} 从 ${o.fromOffset}条到 ${o.untilOffset}条")
          DB.autoCommit {
            implicit session => {
              SQL("replace into offset_storage(topic,groupid,partitions,offset) values(?,?,?,?)")
                .bind(o.topic, "liuge.group.id", o.partition, o.untilOffset).update().apply()
            }
          }
        } else {
          println("!该批次没有消费到数据")
        }
      }
      // ======================保存offset的模板 结束==================================
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
