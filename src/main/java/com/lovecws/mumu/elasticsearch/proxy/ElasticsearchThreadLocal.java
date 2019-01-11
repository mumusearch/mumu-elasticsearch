package com.lovecws.mumu.elasticsearch.proxy;


import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchPool;

/**
 * es客户端本地线程池，如果线程上下文中存在es连接，直接使用。如果不存在则获取，并且在cleanup中做清理
 */
public class ElasticsearchThreadLocal {

    public static ThreadLocal<ElasticsearchClient> threadLocal = new ThreadLocal<ElasticsearchClient>();
    public static ElasticsearchPool elasticsearchPool = new ElasticsearchPool();

    public static ElasticsearchClient get() {
        ElasticsearchClient elasticsearchClient = threadLocal.get();
        //如果上下文中无es客户端连接 则使用连接池创建一个
        if (elasticsearchClient == null) {
            elasticsearchClient = elasticsearchPool.buildClient();
        }
        return elasticsearchClient;
    }

    public static void set(ElasticsearchClient elasticsearchClient) {
        threadLocal.set(elasticsearchClient);
    }

    public static void cleanup() {
        //清理es连接 如果es连接还在线程上下文中则返回到连接池中
        ElasticsearchClient elasticsearchClient = threadLocal.get();
        if (elasticsearchClient != null && !elasticsearchClient.isReturn()) {
            elasticsearchPool.removeClient(elasticsearchClient);
        }
    }
}
