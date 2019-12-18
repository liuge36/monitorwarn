package com.csylh.projectrelated

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * Description:   spark kafka 0.8 对接模板 ，kafka单个分区
  *   spark-streaming-kafka-0-8_2.11
  *
  * offset ： 可以自定义存储在 ZK/HBase/MySQL ...
  *
  * @Author: 留歌36
  * @Date: 2019/12/17 9:20
  */
object OffsetManageApp extends Logging{
    def main(args: Array[String]): Unit = {
//      val conf = new SparkConf().setMaster("local[2]").setAppName("")
      val spark = SparkSession.builder().master("local[2]").appName("OffsetManageApp").getOrCreate()

      val sc = spark.sparkContext
      val ssc = new StreamingContext(sc, Seconds(10))

      val kafkaParams = Map[String, String](
        "metadata.broker.list"->"192.168.1.200:9092",
        "group.id"->"liuge.group.id" ,// 消费的时候是以组为单位进行消费
        "auto.offset.reset" -> "smallest" // 从头消费
      )

      val topics = "onlinelogs2".split(",").toSet

      // 步骤一： 先获取offset
      import scalikejdbc._
      DBs.setup()
      val fromOffsets =
        DB.readOnly {
          implicit session => {
            SQL("select * from offset_storage group by partitions ").map(rs =>
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
            allDF.show()

            // TODO...




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
