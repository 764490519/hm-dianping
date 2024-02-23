package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * Redis缓存工具类
 */
@Slf4j
@Component
public class CacheClient {


    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //设置带过期时间的缓存
    public void set(String key, Object value, Long time, TimeUnit unit){
        String JsonString = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key,JsonString,time,unit);
    }


    //设置带逻辑过期时间的缓存
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //封装RedisData对象（带有逻辑过期时间）
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        //写入Redis
        String JsonString = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key,JsonString);
    }


    //根据id查询数据库，并且写入空对象防止缓存穿透
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //Redis中存在数据，直接返回
            return JSONUtil.toBean(json,type);
        }

        //判断Redis中缓存的是否为空对象(此时shopJson应该为"")
        if(json != null){
            return null;
        }


        //Redis中不存在店铺信息，根据id查询数据库
        R r = dbFallback.apply(id);

        //数据库中也不存在，返回错误，Redis中缓存空对象
        if(r == null){
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //数据库中存在，写入Redis,更新时间30分钟
        this.set(key,r,time,unit);

        //返回数据
        return r;

    }



    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    //根据id查询数据库，并且设置逻辑过期时间防止缓存击穿
    public  <R,ID> R  queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isBlank(json)){
            //Redis中不存在数据，直接返回null
            return null;
        }

        //缓存命中，反序列化，
        RedisData redisData = JSONUtil.toBean(json,RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //判断过期时间,如果信息未过期，直接返回
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }

        //信息已过期，开始缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;


        //尝试获取互斥锁
        boolean isLock = tryLock(lockKey);

        //获取成功，开启独立线程
        if (isLock){
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入Redis
                    this.setWithLogicalExpire(key, r1, time,unit);

                } catch (Exception ex){
                    throw new RuntimeException(ex);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }

        //返回返回过期的信息
        return r;

    }

    //尝试获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //接触互斥锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


}
