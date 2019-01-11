package com.lovecws.mumu.elasticsearch.client;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: es连接池
 * @date 2018-06-08 20:58
 */
public class ElasticsearchPool {

    private static GenericObjectPool<ElasticsearchClient> pool = null;

    //懒汉模式创建连接池单例
    public ElasticsearchPool() {
        if (pool == null) {
            synchronized (ElasticsearchPool.class) {
                if (pool == null) {
                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                    config.setMaxIdle(10);
                    config.setMaxTotal(20);
                    config.setMinIdle(5);
                    config.setTestOnReturn(false);//当es连接返回到连接池的时候是否需要验证es连接
                    config.setTestOnBorrow(false);//当租借es链接的时候是否需要验证es连接
                    config.setTestOnCreate(true);//当创建es链接的时候是否需要验证es连接
                    pool = new GenericObjectPool<ElasticsearchClient>(new ElasticsearchPooledObjectFactory(), config);
                }
            }
        }
    }

    /**
     * 程序中实用到的es链接都需要从这里获取
     *
     * @return es客户端
     */
    public ElasticsearchClient buildClient() {
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    /**
     * 但es链接实用完毕之后，将链接返回到连接池中
     *
     * @param client es客户端
     */
    public void removeClient(ElasticsearchClient client) {
        if (client == null) {
            return;
        }
        client.setReturn(true);
        pool.returnObject(client);
    }
}
