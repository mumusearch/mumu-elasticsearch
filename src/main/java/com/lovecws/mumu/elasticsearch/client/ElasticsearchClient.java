package com.lovecws.mumu.elasticsearch.client;

import com.lovecws.mumu.elasticsearch.common.ElasticsearchConfig;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引管理
 * @date 2018-06-02 21:18
 */
public class ElasticsearchClient {

    public static final Logger log = Logger.getLogger(ElasticsearchClient.class);

    private TransportClient client = null;

    public ElasticsearchClient() {
        client = client();
    }

    public TransportClient client() {
        if (client != null) {
            return client;
        }

        Settings settings = Settings.builder()
                .put("cluster.name", ElasticsearchConfig.getProperty("elasticsearch.cluster.name", "elasticsearch"))
                .put("transport.tcp.compress", ElasticsearchConfig.getBoolean("elasticsearch.transport.tcp.compress", true))
                .put("transport.tcp.compress", ElasticsearchConfig.getBoolean("elasticsearch.transport.tcp.compress", true))
                .put("client.transport.sniff", ElasticsearchConfig.getBoolean("elasticsearch.client.transport.sniff", false))
                .put("client.transport.ping_timeout", ElasticsearchConfig.getProperty("elasticsearch.client.transport.ping_timeout", "30s"))//默认为5s，此参数指定了ping命令响应的超时时间。
                .build();
        String[] hostnames = ElasticsearchConfig.getArray("elasticsearch.transport.host", ",");
        String[] ports = ElasticsearchConfig.getArray("elasticsearch.transport.port", ",");
        if (hostnames == null || ports == null || hostnames.length != ports.length) {
            throw new IllegalArgumentException("elasticsearch.transport.host[" + hostnames + "]、elasticsearch.transport.port[" + ports.toString() + "]配置错误");
        }
        try {
            TransportAddress[] transportAddresses = new TransportAddress[hostnames.length];
            for (int i = 0; i < hostnames.length; i++) {
                transportAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(hostnames[i]), Integer.parseInt(ports[i]));
            }
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(transportAddresses);
            log.info("初始化elasticsearch连接....");
            return client;
        } catch (UnknownHostException e) {
            log.error(e);
            throw new IllegalArgumentException("非法的hostname:" + hostnames + ",port:" + ports);
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
