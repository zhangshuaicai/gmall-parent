package com.atguigu.gmall.realtime.util

import java.io.{InputStream, InputStreamReader}
import java.util.Properties

object PropertiesUtil {
    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.getLoad("config.properties")
        println(properties.getProperty("kafka.broker.list"))
    }
    def getLoad(propertyName:String) ={
        val prop = new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyName),"UTF-8"))
        prop
    }
}
