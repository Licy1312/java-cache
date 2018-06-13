package com.learning.cache.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations ="classpath:spring-application.xml")
public class RedisManagerTest {

	@Autowired
	private RedisManager redisManager;

	@Test
	public void setValueTest() {
		String key = "UNIQUE_ORDER_ID";
		long orderId = redisManager.incrementValue(key);
		System.out.println("订单号:"+orderId);
	}
}
