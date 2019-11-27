package com.atguigu.gmall.gmallpublisher.controllor;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



@RestController
public class DauControllor {

    @Autowired
    DauService dauService;

    @GetMapping("getDauTotal")
    public String getDauTotal(@RequestParam("date") String date){
        Long total = dauService.getDauTotal(date);
        List list = new ArrayList<>();
        Map map = new HashMap<String,String>();
        map.put("id","dau");
        map.put("name","新增日活");
        map.put("value",total);
        list.add(map);
        Map map1 = new HashMap<String,String>();
        map1.put("id","new_mid");
        map1.put("name","新增设备");
        map1.put("value",6);
        list.add(map1);

        String jsonString = JSON.toJSONString(list);

        return jsonString;
    }
}
