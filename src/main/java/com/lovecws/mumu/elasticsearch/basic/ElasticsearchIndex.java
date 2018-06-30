package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchPool;
import com.lovecws.mumu.elasticsearch.common.ElasticsearchMapping;
import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引
 * @date 2018-06-03 11:50
 */
public class ElasticsearchIndex {

    public static final Logger log = Logger.getLogger(ElasticsearchIndex.class);
    public static final ElasticsearchPool pool = new ElasticsearchPool();

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @param aliasName 别名名称
     * @param typeName  类型名称
     * @param mappings  字段映射集合
     */
    public boolean createIndex(String indexName, String aliasName, String typeName, List<MappingEntity> mappings) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            //判断索引是否存在
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            if (existsResponse.isExists()) {
                log.info("index [" + indexName + "] exists!");
                return false;
            }
            CreateIndexResponse createIndexResponse = transportClient.admin().indices()
                    .prepareCreate(indexName)
                    .setSettings(transportClient.settings())
                    .addAlias(new Alias(aliasName))
                    .addMapping(typeName, ElasticsearchMapping.mapping(typeName, mappings))
                    .setUpdateAllTypes(true)
                    .execute()
                    .actionGet();
            if (createIndexResponse.isAcknowledged()) {
                log.info("index [" + indexName + "] create success!");
                return true;
            } else {
                createIndexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return false;
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean exists(String indexName) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            return existsResponse.isExists();
        } finally {
            pool.removeClient(elasticsearchClient);
        }
    }

    /**
     * 删除索引
     *
     * @param indexName 索引的名称
     */
    public boolean deleteIndex(String indexName) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        boolean deleteSuccess = false;
        try {
            DeleteIndexResponse deleteIndexResponse = transportClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            if (deleteIndexResponse.isAcknowledged()) {
                log.info("索引[" + indexName + "] 删除成功");
                deleteSuccess = true;
            } else {
                deleteIndexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return deleteSuccess;
    }

    /**
     * 关闭索引
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean closeIndex(String indexName) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        boolean closeSuccess = false;
        try {
            CloseIndexResponse closeIndexResponse = transportClient.admin().indices().close(new CloseIndexRequest(indexName)).actionGet();
            closeSuccess = true;
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return closeSuccess;
    }

    /**
     * 打开索引
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean openIndex(String indexName) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        boolean openSuccess = false;
        try {
            OpenIndexResponse openIndexResponse = transportClient.admin().indices().open(new OpenIndexRequest(indexName)).actionGet();
            openSuccess = true;
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return openSuccess;
    }


}
