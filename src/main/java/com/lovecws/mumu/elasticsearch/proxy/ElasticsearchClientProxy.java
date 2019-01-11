package com.lovecws.mumu.elasticsearch.proxy;

import net.sf.cglib.proxy.Enhancer;

/**
 * es连接代理
 *
 * @param <T>
 */
public class ElasticsearchClientProxy<T> {

    public T getProxy(Class<T> clazz) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(clazz);
        enhancer.setCallback(new CglibProxyIntercepter());
        T t = (T) enhancer.create();
        return t;
    }
}
