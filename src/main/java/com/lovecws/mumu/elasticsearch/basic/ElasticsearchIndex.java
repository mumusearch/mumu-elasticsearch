package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.common.ElasticsearchConfig;
import com.lovecws.mumu.elasticsearch.common.ElasticsearchMapping;
import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
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
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引
 * @date 2018-06-03 11:50
 */
public class ElasticsearchIndex {

    public static final Logger log = Logger.getLogger(ElasticsearchIndex.class);

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @param aliasName 别名名称
     * @param typeName  类型名称
     * @param mappings  字段映射集合
     */
    public boolean createIndex(String indexName, String aliasName, String typeName, List<MappingEntity> mappings) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            //判断索引是否存在
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            if (existsResponse.isExists()) {
                log.info("index [" + indexName + "] exists!");
                return false;
            }
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", ElasticsearchConfig.getInteger("elasticsearch.index.number_of_shards", 5))
                    .put("index.number_of_replicas", ElasticsearchConfig.getInteger("elasticsearch.index.number_of_replicas", 0))
                    .put("index.refresh_interval", ElasticsearchConfig.getProperty("elasticsearch.index.refresh_interval", "120s"))
                    .build();

            CreateIndexResponse createIndexResponse = transportClient.admin().indices()
                    .prepareCreate(indexName)
                    .setSettings(settings)
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
        }finally {
            ElasticsearchThreadLocal.cleanup();
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
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            return existsResponse.isExists();
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName 索引的名称
     */
    public boolean deleteIndex(String indexName) {
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
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
        }finally {
            ElasticsearchThreadLocal.cleanup();
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
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        boolean closeSuccess = false;
        try {
            CloseIndexResponse closeIndexResponse = transportClient.admin().indices().close(new CloseIndexRequest(indexName)).actionGet();
            closeSuccess = closeIndexResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }finally {
            ElasticsearchThreadLocal.cleanup();
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
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        boolean openSuccess = false;
        try {
            OpenIndexResponse openIndexResponse = transportClient.admin().indices().open(new OpenIndexRequest(indexName)).actionGet();
            openSuccess = openIndexResponse.isAcknowledged();
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        }finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return openSuccess;
    }
}
