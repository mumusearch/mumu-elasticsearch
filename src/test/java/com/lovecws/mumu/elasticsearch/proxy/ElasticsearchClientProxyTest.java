package com.lovecws.mumu.elasticsearch.proxy;

import com.lovecws.mumu.elasticsearch.basic.ElasticsearchIndex;
import junit.framework.TestCase;

/**
 * @program: mumu-elasticsearch
 * @description: 客户端代理测试
 * @author: 甘亮
 * @create: 2019-01-12 17:05
 **/
public class ElasticsearchClientProxyTest extends TestCase {

    public void testProxy() {
        ElasticsearchClientProxy<ElasticsearchIndex> elasticsearchClientProxy = new ElasticsearchClientProxy<>();
        ElasticsearchIndex clientProxyProxy = elasticsearchClientProxy.getProxy(ElasticsearchIndex.class);
        boolean gynetres = clientProxyProxy.exists("gynetres");
        System.out.println(gynetres);
    }
}
