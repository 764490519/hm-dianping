package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 带过期时间的Redis数据封装类
 */
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
