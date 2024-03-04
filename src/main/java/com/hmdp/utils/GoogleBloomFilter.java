package com.hmdp.utils;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Component
public class GoogleBloomFilter {
    @Resource
    IShopService shopService;

    private static final Long capacity = 10000L;
    private static final double errorRate = 0.01;
    private  BloomFilter<Long> filter;


    @PostConstruct
    public void initData(){
        this.filter = BloomFilter.create(Funnels.longFunnel(), capacity, errorRate);
        List<Shop> list = shopService.list();
        for(Shop shop : list){
            filter.put(shop.getId());
        }
    }

    public void put(Long id){
        filter.put(id);
    }

    public boolean mightContains(Long id){
        return filter.mightContain(id);
    }
}
