package com.lovecws.mumu.elasticsearch.proxy;

import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchPool;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;

/**
 * 使用cglib代理获取es连接
 */
public class CglibProxyIntercepter implements MethodInterceptor {

    public static final Logger log = Logger.getLogger(CglibProxyIntercepter.class);
    public ElasticsearchPool elasticsearchPool = new ElasticsearchPool();

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        before();
        Object object = null;
        try {
            object = methodProxy.invokeSuper(o, objects);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            //将连接返回到连接池
            after();
        }

        return object;
    }

    /**
     * 代理之前处理的事情 获取到es连接
     */
    public void before() {
        ElasticsearchClient elasticsearchClient = elasticsearchPool.buildClient();
        ElasticsearchThreadLocal.set(elasticsearchClient);
    }

    /**
     * 代理之后处理的事情 关闭es连接
     */
    public void after() {
        ElasticsearchClient elasticsearchClient = ElasticsearchThreadLocal.get();
        elasticsearchPool.removeClient(elasticsearchClient);
        ElasticsearchThreadLocal.threadLocal.remove();
    }
}
