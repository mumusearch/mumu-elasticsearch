package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.common.ElasticsearchMapping;
import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.apache.log4j.Logger;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.Map;

/**
 * @program: mumu-elasticsearch
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-04-25 10:22
 **/
public class ElasticsearchUpdate {

    public static final Logger log = Logger.getLogger(ElasticsearchUpdate.class);

    /**
     * 文档不存在则创建、否则更新
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param id        主键
     * @param valueMap  值映射
     */
    public int upsert(String indexName, String typeName, String id, Map<String, Object> valueMap) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        if (valueMap == null) return RestStatus.BAD_REQUEST.getStatus();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .doc(ElasticsearchMapping.content(valueMap))
                    //.docAsUpsert(true)
                    .upsert(ElasticsearchMapping.content(valueMap));
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /**
     * script更新
     *
     * @param indexName    索引名称
     * @param typeName     类型名称
     * @param id           主键
     * @param scriptSource 脚本
     * @param scriptParams 参数
     *                     POST test/_update/1
     *                     {
     *                     "script" : {
     *                     "source": "ctx._source.counter += params.count)",
     *                     "lang": "painless",
     *                     "params" : {
     *                     "tag" : "blue"
     *                     }
     *                     }
     *                     }
     */
    public int script(String indexName, String typeName, String id, String scriptSource, Map<String, Object> scriptParams) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .script(new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams));
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /**
     * 当文档不存在的时候 直接添加文档 valueMap
     *
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param id           索引id
     * @param scriptSource 脚本
     * @param scriptParams 脚本参数
     * @param valueMap     新添加的文档值
     *                     {
     *                     "script" : {
     *                     "source": "ctx._source.counter += params.count",
     *                     "lang": "painless",
     *                     "params" : {
     *                     "count" : 4
     *                     }
     *                     },
     *                     "upsert" : {
     *                     "counter" : 1
     *                     }
     *                     }
     * @return
     */
    public int scriptUpsert(String indexName, String typeName, String id, String scriptSource, Map<String, Object> scriptParams, Map<String, Object> valueMap) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .script(new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams))
                    .upsert(ElasticsearchMapping.content(valueMap))
                    .scriptedUpsert(false);//设为true 当文档是否存在都执行script脚本
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }
}
