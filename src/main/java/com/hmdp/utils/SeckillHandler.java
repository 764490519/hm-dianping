package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;

/**
 * 异步处理订单工具类
 */
@Component
public class SeckillHandler {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedissonClient redissonClient;
    @KafkaListener(topics = {"seckill_order"})
    public void consumer(ConsumerRecord<String, String> consumerRecord) {
        String value = consumerRecord.value();
        VoucherOrder voucherOrder = JSONUtil.toBean(value, VoucherOrder.class);
        //获取分布式锁

        //获取失败，重试

        //获取成功，更新数据库
        voucherOrderService.save(voucherOrder);
        seckillVoucherService.update();
    }

}