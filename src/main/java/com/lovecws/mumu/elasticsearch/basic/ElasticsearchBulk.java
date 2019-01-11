package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.common.ElasticsearchMapping;
import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: bulk操作
 * @date 2018-06-03 18:24
 */
public class ElasticsearchBulk {

    public static final Logger log = Logger.getLogger(ElasticsearchBulk.class);

    /**
     * 创建文档
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param valueMap  值映射
     */
    public void index(String indexName, String typeName, Map<String, Object> valueMap) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            String uuid = UUID.randomUUID().toString().replace("-", "");
            IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex();
            indexRequestBuilder.setIndex(indexName);
            indexRequestBuilder.setType(typeName);
            indexRequestBuilder.setId(uuid);
            indexRequestBuilder.setOpType(IndexRequest.OpType.INDEX);
            indexRequestBuilder.setRouting(uuid);
            indexRequestBuilder.setSource(ElasticsearchMapping.content(valueMap));
            IndexResponse indexResponse = indexRequestBuilder.get();
            if (indexResponse.status().getStatus() == 200) {
                log.info("文档创建成功！");
            } else {
                indexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }finally {
            ElasticsearchThreadLocal.cleanup();
        }
    }

    /**
     * 批量插入
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param values    值映射集合
     */
    public void bulk(String indexName, String typeName, List<Map<String, Object>> values) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
            for (Map<String, Object> valueMap : values) {
                IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex();
                indexRequestBuilder.setIndex(indexName);
                indexRequestBuilder.setType(typeName);
                indexRequestBuilder.setId(UUID.randomUUID().toString().replace("-", ""));
                indexRequestBuilder.setOpType(IndexRequest.OpType.INDEX);
                indexRequestBuilder.setSource(ElasticsearchMapping.content(valueMap));
                bulkRequestBuilder.add(indexRequestBuilder);
            }
            BulkResponse bulkItemResponses = bulkRequestBuilder.get();
            if (bulkItemResponses.hasFailures()) {
                log.error(bulkItemResponses.buildFailureMessage());
            } else {
                log.info("bulk insert success!");
            }
        } catch (Exception e) {
            log.error(e);
        }finally {
            ElasticsearchThreadLocal.cleanup();
        }
    }

}
