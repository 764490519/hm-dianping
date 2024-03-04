---
--- Created by 26793.
--- DateTime: 2024/2/22 14:54
---

--1参数列表
--1.1优惠券id
local voucherId = ARGV[1]
--1.2用户id
local userId = ARGV[2]
--1.3订单id
--local orderId = ARGV[3]

--2数据key
--2.1库存key
local stockKey = 'seckill:stock:' .. voucherId
--2.2订单key
local orderKey = 'seckill:order:' .. voucherId

--3脚本业务
--3.1判断库存是否充足
if tonumber(redis.call('get',stockKey))  <= 0 then
	--库存不足
	return 1
end

--3.2判断用户是否下单
if (redis.call('sismember',orderKey,userId) == 1) then
	--存在用户id，是重复下单
	return 2
end

--3.3扣库存
redis.call('incrby',stockKey,-1)

--3.4下单
redis.call('sadd',orderKey,userId)

--3.6发送消息到队列中 XADD stream.orders * k1 v1 k2 v2 ...
--redis.call('xadd','stream.orders', '*', 'userId',userId,'voucherId',voucherId,'id',orderId);

return 0