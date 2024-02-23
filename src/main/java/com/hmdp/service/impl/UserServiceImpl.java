package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static cn.hutool.core.bean.BeanUtil.beanToMap;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     *发送手机验证码
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {

        //校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){

            //不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }

        //符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到Redis,有效期2分钟 （set key value ex 120）
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //发送验证码
        log.debug("发送短信验证码成功，验证码: " + code);


        return Result.ok();
    }


    /**
     *登陆验证
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {

        //校验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){

            return Result.fail("手机号格式错误");
        }

        //从Redis 获取验证码，并验证
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.equals(code)){
            return Result.fail("验证码错误");
        }

        //一致，根据手机号查询用户
        User user = query().eq("phone",phone).one();

        //判断用户是否存在
        if(user == null){
            //不存在，创建新用户，保存到数据库
            user = createUserWithPhone(phone);
        }


        //保存用户信息到Redis
        String token = UUID.randomUUID().toString(true);
        UserDTO userDTO =  BeanUtil.copyProperties(user, UserDTO.class);
        Map<String,Object> userMap =  BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));

        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY + token,userMap);

        //设置有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);


        //返回token
        return Result.ok(token);
    }


    /**
     *通过手机号创建用户
     */
    private User createUserWithPhone(String phone) {


        //创建用于对象
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        //保存
        save(user);

        return user;
    }
}
