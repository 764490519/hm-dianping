package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;


    @Override
    public Object queryById(Long id) {
        //缓存穿透
//        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY,id, Shop.class,this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY,id, Shop.class,this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.SECONDS);
        if (shop == null){
            return Result.fail("店铺不存在");
        }

        //返回
        return  Result.ok(shop);
    }

    //重建缓存
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //查询用户数据
        Shop shop = getById(id);

        //模拟重建缓存延迟
        Thread.sleep(200);

        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }



    //通过互斥锁解决缓存击穿的查询
//    private Shop queryWithMutex(Long id) {
//        String key = RedisConstants.CACHE_SHOP_KEY + id;
//
//        //尝试从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //Redis中存在数据，直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//
//        //判断Redis中缓存的是否为空对象(此时shopJson应该为"")
//        if (shopJson != null) {
//            return null;
//        }
//
//
//        //实现缓存重建，获取互斥锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLocked = tryLock(lockKey);
//
//            //获取失败，休眠并充实
//            if (!isLocked) {
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//
//            //获取成功，，根据id查询数据库
//            shop = getById(id);
//
//            //模拟重建延时
//            Thread.sleep(200);
//
//            //数据库中也不存在，返回错误，Redis中缓存空对象
//            if (shop == null) {
//                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//
//            //数据库中存在，写入Redis,更新时间30分钟
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        } catch (InterruptedException ex) {
//            throw new RuntimeException(ex);
//        } finally {
//            //释放互斥锁
//            unLock(lockKey);
//        }
//
//        //返回数据
//        return shop;
//
//    }


//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //通过设置逻辑过期时间解决缓存击穿的查询
//    private Shop queryWithLogicalExpire(Long id){
//        String key = RedisConstants.CACHE_SHOP_KEY + id;
//
//        //尝试从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //判断是否存在
//        if(StrUtil.isBlank(shopJson)){
//            //Redis中不存在数据，直接返回null
//            return null;
//        }
//
//        //缓存命中，反序列化，
//        RedisData redisData = JSONUtil.toBean(shopJson,RedisData.class);
//        JSONObject jsonObject = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        //判断过期时间,如果信息未过期，直接返回
//        if(expireTime.isAfter(LocalDateTime.now())){
//            return shop;
//        }
//
//        //信息已过期，开始缓存重建
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//
//
//        //尝试获取互斥锁
//        boolean isLock = tryLock(lockKey);
//
//        //获取成功，开启独立线程
//        if (isLock){
//            CACHE_REBUILD_EXECUTOR.submit(() ->{
//                try {
//                    //重建缓存
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception ex){
//                    throw new RuntimeException(ex);
//                }finally {
//                    //释放锁
//                    unLock(lockKey);
//                }
//            });
//        }
//
//        //返回返回过期的商铺信息
//        return shop;
//
//    }

    //解决缓存穿透的查询
//    private Shop queryWithPassThrough(Long id){
//        String key = RedisConstants.CACHE_SHOP_KEY + id;
//
//        //尝试从Redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
//            //Redis中存在数据，直接返回
//            return JSONUtil.toBean(shopJson,Shop.class);
//        }
//
//        //判断Redis中缓存的是否为空对象(此时shopJson应该为"")
//        if(shopJson != null){
//            return null;
//        }
//
//
//        //Redis中不存在店铺信息，根据id查询数据库
//        Shop shop = getById(id);
//
//        //数据库中也不存在，返回错误，Redis中缓存空对象
//        if(shop == null){
//            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
//            return null;
//        }
//
//        //数据库中存在，写入Redis,更新时间30分钟
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        //返回数据
//        return shop;
//    }

    //尝试获取锁
//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    //接触互斥锁
//    private void unLock(String key){
//        stringRedisTemplate.delete(key);
//    }


    //更新商户
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺ID不能为空");
        }
        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
