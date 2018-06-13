package com.learning.cache.redis;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * Redis管理类
 * @Author Lee
 */
@Slf4j
@Component
public class RedisManager {

    /**
     * redis处理模板
     */
    @Autowired
    private StringRedisTemplate redisTemplate;



    /**
     * 查询redis数据库,指定返回类型 object
     *
     * @param key   查询关键字
     * @param clazz 指定返回对象类型
     * @param <T>   返回对象类型,泛型
     * @return 返回对象
     */
    public <T> T queryObjectByKey(final String key, final Class<T> clazz) {
        log.debug("redis queryObjectByKey request:{}", key);
        String resultStr = queryStringByKey(key);
        if (StringUtils.isEmpty(resultStr)) {
            return null;
        }
        T value = JSONObject.parseObject(resultStr, clazz);
        log.debug("redis queryObjectByKey response:{}", value.toString());
        return value;
    }


    /**
     * 查询redis 数据库 返回List
     *
     * @param key 查询关键字
     * @param clazz   指定返回List内存放的对象类型
     * @param <T>     返回对象类型,集合泛型
     * @return List<T>      返回对象集合
     */
    public <T> List<T> queryListByKey(final String key, final Class<T> clazz) {
        log.debug("redis queryListByKey request：{}", key);
        String resultStr = queryStringByKey(key);
        if (StringUtils.isEmpty(resultStr)) {
            return null;
        }
        List<T> value = JSONObject.parseArray(resultStr, clazz);
        log.debug("redis queryListByKey response：{}", value);
        return value;
    }


    /**
     * 插入redis 数据库,设置有效期
     *
     * @param obj     保存对象
     * @param key     关键字
     * @param timeout 有效期（毫秒）
     * @return 对象类型, 泛型
     */
    public boolean insertObject(final Object obj, final String key, final long timeout) {
        log.debug("redis insertObject request:key={},obj={}", key, obj.toString());
        try {
            byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
            byte[] redisValue = redisTemplate.getStringSerializer().serialize(toJSONString(obj));
            boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
                @Override
                public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                    connection.set(redisKey, redisValue);
                    if (timeout > 0) {
                        connection.pSetEx(redisKey, timeout,redisValue);
                    }else{
                        connection.set(redisKey, redisValue);
                    }
                    return true;
                }
            });
            log.debug("redis insertObject response：{}", result);
            return result;
        } catch (Exception e) {
            log.error("redis insertObject exception:{}", e);
            return false;
        }
    }


    /**
     * 删除redis 保存对象
     *
     * @param key 查询关键字
     * @return true\false
     */
    public boolean deleteObject(final String key) {
        log.debug("redisManager deleteObject request:key={}", key);
        try {
            byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
            Long result = redisTemplate.execute(new RedisCallback<Long>() {
                @Override
                public Long doInRedis(RedisConnection connection) throws DataAccessException {
                    return connection.del(redisKey);
                }
            });
            log.debug("redisManager deleteObject response：{}", result);
            return result > 0;
        } catch (Exception e) {
            log.error("redis deleteObject exception:{}", e);
            return false;
        }
    }


    /**
     * redis生产自增唯一Id
     * @param key 关键值key
     * @return 当前ID
     */
    public long incrementValue(final String key) {
        final byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
        long result = redisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                long uniqueValue = redisConnection.incr(redisKey);
                return uniqueValue;
            }
        });
        return result;
    }


    /**
     * 查询是否存在key
     * @param key 指的的key值
     * @return true|false
     */
    public boolean isExistKey(final String key) {
        byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
        boolean flag = (Boolean)redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                byte[] value = redisConnection.get(redisKey);
                return value != null;
            }
        });
        return flag;
    }


    /**
     * 判断Key值是否存在,若不存在则保存当前值并成功返回true，其他情况返回false
     *
     * @param key     查询关键字
     * @param timeout 过期时间(MILLISECONDS)
     * @return true|false
     */
    public boolean setIfNotExist(final String key, final long timeout) {
        log.debug("lockRedis request:key={}", key);
        if (timeout <= 0) {
            log.warn("lockRedis lock time must gt 0");
            return false;
        }
        try {
            byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
            boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
                @Override
                public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                    Boolean lock = false;
                    try {
                        lock = connection.setNX(redisKey, new byte[1]);
                        if(!lock){
                            //key has existed
                            return lock;
                        }
                        log.debug("lockRedis Lock Result:{}", lock);
                        Boolean setTimeOutResult = connection.pExpire(redisKey,timeout);
                        log.debug("lockRedis setTimeOutResult TimeOut:{} Result:{}", timeout, setTimeOutResult);
                        if (lock && setTimeOutResult) {
                            return true;
                        }
                        //insert fail,then delete key
                        if (!setTimeOutResult) {
                            connection.del(redisKey);
                            return false;
                        }
                    } catch (Exception e) {
                        if (lock) {
                            connection.del(redisKey);
                        }
                        log.warn("lockRedis Fail Exception:{}", e);
                    }
                    return false;
                }
            });

            log.debug("lockRedis request Result:{}", result);
            return result;
        } catch (Exception e) {
            log.error("lockRedis exception, e:{}", e);
            return false;
        }
    }

    /**
     * 查询redis数据库 对象以字符串的形式返回
     *
     * @param key 查询关键字
     * @return 返回对象字符串
     */
    private String queryStringByKey(final String key) {
        log.debug("redis queryStringByKey request:{}", key);
        try {
            byte[] redisKey = redisTemplate.getStringSerializer().serialize(key);
            String resultStr = (String) redisTemplate.execute(new RedisCallback<Object>() {
                @Override
                public String doInRedis(RedisConnection connection) throws DataAccessException {
                    byte[] value = connection.get(redisKey);
                    if (value == null) {
                        return null;
                    }
                    return redisTemplate.getStringSerializer().deserialize(value);
                }
            });
            log.debug("redis queryStringByKey response:{}", resultStr);
            return resultStr;
        } catch (Exception e) {
            log.error("redis query exception:{}", e);
            return null;
        }
    }

    /**
     * redis 值如果是对象转成json,如果是字符串不变
     *
     * @param obj 值
     * @return 值
     */
    private String toJSONString(Object obj) {
        final String value;
        if (obj instanceof String) {
            value = obj + "";
        } else {
            value = JSONObject.toJSONString(obj);
        }
        return value;
    }

}
