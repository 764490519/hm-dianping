package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    //用于区分进程，不同进程的ID前缀不同，防止误删除锁
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    //加载Lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }


    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //尝试获取锁
    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程表示
        String threadId = ID_PREFIX  + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name,threadId,timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }


    //调用Lua脚本释放锁，保证原子性
    public void unlock() {
        //调用Lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }


    //删除键值对释放锁
//    @Override
//    public void unLock() {
//        //获取线程标识
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        //获取锁中的标识
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//
//        //判断标识是否一致
//        if(threadId.equals(id)){
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
