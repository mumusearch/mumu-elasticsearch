package com.lovecws.mumu.elasticsearch.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Test;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: esClient测试
 * @date 2018-06-30 8:27
 */
public class ElasticsearchClientTest {

    @Test
    public void client() {
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient();
        TransportClient client = elasticsearchClient.client();
        List<DiscoveryNode> discoveryNodes = client.connectedNodes();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            System.out.println(discoveryNode);
        }
    }
}
