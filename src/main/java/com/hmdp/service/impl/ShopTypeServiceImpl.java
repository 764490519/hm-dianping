package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public List<ShopType> getShopTypeList() {

        String key = "shopType:";

        //尝试从Redis获取数据
        List<String> JsonList = stringRedisTemplate.opsForList().range(key,0,-1);
        List<ShopType> shopTypesList = new ArrayList<>();
        for(String str : JsonList){
            shopTypesList.add(JSONUtil.toBean(str,ShopType.class));
        }

        //Redis中有数据，返回
        if(!shopTypesList.isEmpty()){
            return  shopTypesList;
        }

        //Redis中无数据，在MySql中查找
        shopTypesList = query().orderByAsc("sort").list();

        //储存在缓存中
        for (ShopType shopType : shopTypesList){
            JsonList.add(JSONUtil.toJsonStr(shopType));
        }
        stringRedisTemplate.opsForList().rightPushAll(key,JsonList);


        //返回数据
        return shopTypesList;
    }
}
