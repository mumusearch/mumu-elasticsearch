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

    public ElasticsearchPool() {
        if (pool == null) {
            synchronized (ElasticsearchPool.class){
                GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                config.setMaxIdle(10);
                config.setMaxTotal(20);
                config.setMinIdle(5);
                config.setTestOnReturn(false);
                config.setTestOnReturn(false);
                config.setTestOnCreate(true);
                pool = new GenericObjectPool(new ElasticsearchPooledObjectFactory(), config);
            }
        }
    }

    public ElasticsearchClient buildClient() {
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    public void removeClient(ElasticsearchClient client) {
        if (client == null) {
            return;
        }
        pool.returnObject(client);
    }
}
