package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    //全局id生成器
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    //秒杀脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }



    //创建线程池,处理下单请求
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //在类初始化完毕后执行，保证处理程序一直运行
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        //将订单信息保存到数据库
        @Override
        public void run() {
            while (true){
                try {
                    //获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断获取消息是否成功
                    if(list == null || list.isEmpty()){
                        //获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true){
                try {
                    //获取PendingList中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1  STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断获取消息是否成功
                    if(list == null || list.isEmpty()){
                        //获取失败，说明pending-list中没有消息，结束循环
                        break;
                    }
                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                    try {
                        //休眠一段时间，继续循环尝试处理
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }


//    //创建阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandler implements Runnable{
//        //将订单信息保存到数据库，使用阻塞队列
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    //获取阻塞队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean isLock = lock.tryLock();
        //判断是否成功
        if(!isLock){
            //获取锁失败，返回错误
            log.error("不允许重复下单");
            return;
        }
        try {
            //事务本质上是通过代理对象实现，需要获取事务代理对象
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            //释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    //基于Redis完成秒杀,使用stream消息队列
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        Long orderId = redisIdWorker.nextId("order");

        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),orderId.toString()
        );

        //结果不为0，无购买资格
        int r = result.intValue();
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //返回订单id
        return Result.ok(orderId);

    }


//    //基于Redis完成秒杀,使用Java阻塞队列
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//
//        //执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//
//        //结果不为0，无购买资格
//        int r = result.intValue();
//        if(r != 0){
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        //结果为0，有购买资格，把下单信息保存到阻塞队列
//        //创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //1.订单id
//        Long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.用户id
//        voucherOrder.setUserId(userId);
//        //3.代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //4.订单保存到阻塞队列
//        orderTasks.add(voucherOrder);
//
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        //返回订单id
//        return Result.ok(orderId);
//
//    }


    public void createVoucherOrder(VoucherOrder voucherOrder) {

        //查询订单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = query().eq("user_id",userId).eq("voucher_id", voucherId).count();
        //判断是否存在
        if(count > 0){
            //用户已经购买过
            log.error("该用户已经购买过一次！");
        }

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")//set stock = stock - 1
                .eq("voucher_id", voucherId).gt("stock", 0)//where id = ? and stock > 0
                .update();

        if(!success){
            //扣减失败，库存不足
            log.error("库存不足");
        }

        //4.订单写入数据库
        save(voucherOrder);
    }



//    基于数据库和分布式锁完成秒杀
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//
//        //判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//
//        //判断库存是否充足
//        if(voucher.getStock() < 1){
//            return Result.fail("库存不足");
//        }
//
//        //一人一单判断,使用悲观锁,防止一个用户同时发起大量请求，绕过判断
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //尝试获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否成功
//        if(!isLock){
//            //获取锁失败，返回错误
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            //事务本质上是通过代理对象实现，需要获取事务代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//
//    }

//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//
//        //查询订单
//        Long userId = UserHolder.getUser().getId();
//        int count = query().eq("user_id",userId).eq("voucher_id", voucherId).count();
//        //判断是否存在
//        if(count > 0){
//            //用户已经购买过
//            return Result.fail("该用户已经购买过一次！");
//        }
//
//
//        //扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")//set stock = stock - 1
//                .eq("voucher_id", voucherId).gt("stock", 0)//where id = ? and stock > 0
//                .update();
//
//        if(!success){
//            //扣减失败，库存不足
//            return Result.fail("库存不足");
//        }
//
//        //创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //1.订单id
//        Long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.用户id
//        voucherOrder.setUserId(userId);
//        //3.代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //4.订单写入数据库
//        save(voucherOrder);
//
//        //返回订单id
//        return Result.ok(orderId);
//    }
}
