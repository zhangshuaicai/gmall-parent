package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmall.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        Long total = dauMapper.getDauTotal(date);
        return total;
    }
}
