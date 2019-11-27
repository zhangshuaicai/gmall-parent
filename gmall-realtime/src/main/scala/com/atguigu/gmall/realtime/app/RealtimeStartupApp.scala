package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.Constants
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realtime")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc,Seconds(5))

        val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.KAFKA_TOPIC_STARTUP,ssc)
        startupStream.map(_.value()).foreachRDD{
            rdd=>{
                println(rdd.collect().mkString("\n"))
            }
        }
        /*val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map(log => {
            val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
            startUpLog
        })*/
        ssc.start()
        ssc.awaitTermination()

    }
}

case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     )
