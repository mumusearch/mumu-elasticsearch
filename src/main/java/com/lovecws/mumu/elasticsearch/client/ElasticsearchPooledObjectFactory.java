package com.lovecws.mumu.elasticsearch.client;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: es连接池工厂
 * @date 2018-06-08 21:09
 */
public class ElasticsearchPooledObjectFactory implements PooledObjectFactory<ElasticsearchClient> {

    @Override
    public PooledObject<ElasticsearchClient> makeObject() throws Exception {
        return new DefaultPooledObject(new ElasticsearchClient());
    }

    @Override
    public void destroyObject(PooledObject<ElasticsearchClient> p) throws Exception {
        ElasticsearchClient client = p.getObject();
        if (client != null) {
            client.close();
        }
    }

    @Override
    public boolean validateObject(PooledObject<ElasticsearchClient> p) {
        return true;
    }

    @Override
    public void activateObject(PooledObject<ElasticsearchClient> p) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<ElasticsearchClient> p) throws Exception {
    }
}
