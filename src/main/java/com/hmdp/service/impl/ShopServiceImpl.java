package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.*;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;


    @Resource
    GoogleBloomFilter bloomFilter;


    @Override
    public Object queryById(Long id) {
        //缓存穿透
//        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY,id, Shop.class,this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //使用布隆过滤器解决缓存穿透
        if(!bloomFilter.mightContains(id)){
            return Result.fail("店铺不存在");
        }

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


    //根据type查询，可以选择是否根据地理坐标查询和排序
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //判断是否需要根据坐标查询
        if(x == null || y ==null){
            // 不需要根据坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //查询redis，根据距离排序、分页。结果：shopId，distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()  // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        //解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from){
            //没有新数据了，结束
            return Result.ok(Collections.emptyList());
        }
        //截取从from到end的部分，进行分页
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>();
        list.stream().skip(from).forEach(result ->{
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //根据id查询
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id",ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //返回
        return Result.ok(shops);
    }
}
