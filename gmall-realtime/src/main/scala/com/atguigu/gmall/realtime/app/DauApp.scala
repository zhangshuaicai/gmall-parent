package com.atguigu.gmall.realtime.app

import java.util.Date
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.Constants
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 实时处理日志的日活数据，通入redis中
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //从kafka读取流文件
        val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.KAFKA_TOPIC_STARTUP,ssc)

        //将流转换成StartUpLog格式的一批RDD
        val startuplogDstream: DStream[StartUpLog] = inputStream.map {
            record =>
                val jsonStr: String = record.value()
                val startupLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
                val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
                val dateArr: Array[String] = dateTimeStr.split(" ")
                startupLog.logDate = dateArr(0)
                startupLog.logHour = dateArr(1)
                startupLog
        }
        //将数据缓存
        startuplogDstream.cache()

        //对数据进行过滤   去重
        val filteredDstream: DStream[StartUpLog] = startuplogDstream.transform { rdd =>
            val jedis: Jedis = RedisUtil.getJedisClient
            val datestr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val key: String = "dau:" + datestr
            val dauMidSet: util.Set[String] = jedis.smembers(key)
            jedis.close()

            //将redis中的midSet放入广播变量
            val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
            println("过滤前：" + rdd.count())

            val filterRDD: RDD[StartUpLog] = rdd.filter {
                startuplog =>
                    val dayMidSet: util.Set[String] = dauMidBC.value
                    !dayMidSet.contains(startuplog.mid)
            }
            println("过滤后：" + filterRDD.count())
            filterRDD
        }
        //先按key进行分组
        val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startUplog=>(startUplog.mid,startUplog)).groupByKey()
        //进行去重，只保留每个设备记录的第一条
        val distictDstream: DStream[StartUpLog] = groupbyMidDstream.flatMap {
            case (mid, startUpLogIt) =>
                startUpLogIt.toList.take(1)
        }
        //将数据添加到redis中
        distictDstream.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    startuplogIt =>
                        val jedis: Jedis = RedisUtil.getJedisClient
                        for (startuplog <- startuplogIt) {
                            val key = "dau:" + startuplog.logDate
                            jedis.sadd(key, startuplog.mid)
                            println(startuplog)
                        }
                        jedis.close()
                }
        }
        import org.apache.phoenix.spark._
        //将过滤之后的数据添加到hbase+phoenix
        distictDstream.foreachRDD(rdd=>{
            rdd.saveToPhoenix("GMALL_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
