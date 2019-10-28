package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.common.ElasticsearchConfig;
import com.lovecws.mumu.elasticsearch.common.ElasticsearchMapping;
import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引模板, 创建索引模板之后，当插入数据的时候使用索引名称匹配索引模板，如果匹配上就使用索引模板的配置来创建索引，减少创建索引的业务逻辑
 * 同时也避免因为索引创建失败而导致的索引分词、分片、副本等的不一致。
 * @date 2018-06-03 11:50
 */
public class ElasticsearchTemplate {

    public static final Logger log = Logger.getLogger(ElasticsearchTemplate.class);

    /**
     * 创建索引模板
     *
     * @param templateName 模板名称
     * @param aliasName    别名
     * @param typeName     模板基类型
     * @param mappings     映射
     * @return 模板是否创建成功
     */
    public boolean createTemplate(String templateName, String aliasName, String typeName, List<MappingEntity> mappings) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            //判断索引模板是否存在
            GetIndexTemplatesResponse indexTemplatesResponse = transportClient.admin().indices().getTemplates(new GetIndexTemplatesRequest(templateName)).actionGet();
            if (indexTemplatesResponse.getIndexTemplates().size() > 0) {
                log.info("template [" + templateName + "] exists!");
                return false;
            }
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", ElasticsearchConfig.getInteger("elasticsearch.index.number_of_shards", 5))
                    .put("index.number_of_replicas", ElasticsearchConfig.getInteger("elasticsearch.index.number_of_replicas", 0))
                    .put("index.refresh_interval", ElasticsearchConfig.getProperty("elasticsearch.index.refresh_interval", "30s"))
                    .build();

            PutIndexTemplateResponse templateResponse = transportClient.admin().indices()
                    .preparePutTemplate(templateName)
                    .setTemplate(templateName)
                    .setSettings(settings)
                    .addAlias(new Alias(aliasName))
                    .addMapping(typeName, ElasticsearchMapping.mapping(typeName, mappings))
                    .get();
            if (templateResponse.isAcknowledged()) {
                log.info("template [" + templateName + "] create success!");
                return true;
            }
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return false;
    }

    /**
     * 根据索引模板的名称获取到索引模板的详细信息
     *
     * @param templateName 索引模板名称
     * @return 索引模板元数据
     */
    public List<IndexTemplateMetaData> getTemplate(String... templateName) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            //获取匹配的索引模板
            GetIndexTemplatesResponse indexTemplatesResponse = transportClient.admin().indices().getTemplates(new GetIndexTemplatesRequest(templateName)).actionGet();
            return indexTemplatesResponse.getIndexTemplates();
        } catch (Exception e) {
            log.error(e);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return null;
    }
}
