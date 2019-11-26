package com.atguigu.gmall.logger.gmalllogger.controllor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String log(@RequestParam("logString") String logString){

        JSONObject jsonObject = JSON.parseObject(logString);
        //获取当前时间戳
        jsonObject.put("ts",System.currentTimeMillis());

        String s = JSONObject.toJSONString(jsonObject);
        //落盘日志文件
        log.info(s);

        //发送数据到kafka
        if (jsonObject.get("type").equals("startup")){
            kafkaTemplate.send(Constants.KAFKA_TOPIC_STARTUP,s);
        }else {
            kafkaTemplate.send(Constants.KAFKA_TOPIC_EVENT,s);
        }

        return "ok";
    }
}
