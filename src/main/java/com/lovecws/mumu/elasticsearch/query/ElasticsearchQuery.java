package com.lovecws.mumu.elasticsearch.query;

import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 查询
 * @date 2018-06-07 20:04
 */
public class ElasticsearchQuery extends ElasticsearchBaseQuery {

    public ElasticsearchQuery(String[] indexNames, String typeName) {
        super(indexNames, typeName);
    }

    /**
     * get查询 使用get查询会很快的查询到数据  直接对id进行hash取模分片数量 直接定位到相应的分片上 然后直接到对应的分片的服务器上查询数据即可
     * 这种方式非常依赖于分片的routing ，如果插入数据的时候指定不同的routing 则使用get查询不到数据
     *
     * @param id 主键
     * @return
     */
    public Map<String, Object> queryById(String id) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            GetRequest getRequest = new GetRequest(indexNames[0], typeName, id);
            GetResponse getResponse = transportClient.get(getRequest).get();
            if (getResponse.isExists()) {
                return getResponse.getSource();
            }
            log.info("文档不存在{indexName:" + indexNames + ",typeName:" + typeName + ",id:" + id + "}");
            return new HashMap<>();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return null;
    }

    /**
     * 使用scroll分页获取es数据
     *
     * @param fieldName   过滤字段名称
     * @param fieldValue  过滤字段值
     * @param currentPage 当前页
     * @param pageSize    一页大小
     * @return
     */
    public List<Map<String, Object>> getPageByScroll(String fieldName, Object fieldValue, int currentPage, int pageSize) {
        QueryBuilder queryBuilder = null;
        if (fieldName != null && fieldValue != null) {
            queryBuilder = new TermQueryBuilder(fieldName, fieldValue);
        }
        return getPageByScroll(queryBuilder, currentPage, pageSize);
    }

    /**
     * 匹配所有查询
     *
     * @return
     */
    public List<Map<String, Object>> matchAllQuery() {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        return query(matchAllQueryBuilder);
    }

    /**
     * 多id查询
     *
     * @param ids
     * @return
     */
    public List<Map<String, Object>> idsQuery(String... ids) {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        idsQueryBuilder.addIds(ids);
        return query(idsQueryBuilder);
    }

    /**
     * term字段查询 不对查询字段进行分词 将字段值 进行term匹配 如果该字段没有分词 则直接匹配字段值
     *
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> termQuery(String fieldName, Object fieldValue) {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(fieldName, fieldValue);
        return query(termQueryBuilder);
    }

    /**
     * term 多值查询
     *
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> termsQuery(String fieldName, Object... fieldValue) {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(fieldName, fieldValue);
        return query(termsQueryBuilder);
    }

    /**
     * common termquery
     *
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> commonTermsQuery(String fieldName, Object fieldValue) {
        QueryBuilder queryBuilder = QueryBuilders.commonTermsQuery(fieldName, fieldValue);
        return query(queryBuilder);
    }

    /**
     * 匹配查询 会对filed进行分词操作，然后在查询 ；
     * 如果字段没有分词 则等同于termquery，否则先将查询值分词，然后在匹配分词term
     * 默认的中文分词器 将汉字逐子分词
     *
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> matchQuery(String fieldName, Object fieldValue) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(fieldName, fieldValue);
        return query(matchQueryBuilder);
    }

    /**
     * boolean关联匹配查询
     *
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> matchBooleanQuery(String fieldName, Object fieldValue, String fieldName2, Object fieldValue2) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery(fieldName, fieldValue))
                .mustNot(QueryBuilders.matchQuery(fieldName2, fieldValue2));
        return query(boolQueryBuilder);
    }

    /**
     * 前缀查询
     *
     * @param fieldName
     * @param prefix
     * @return
     */
    public List<Map<String, Object>> prefixQuery(String fieldName, String prefix) {
        QueryBuilder queryBuilder = QueryBuilders.prefixQuery(fieldName, prefix);
        return query(queryBuilder);
    }

    /**
     * 模糊查询
     *
     * @param fieldName
     * @param fuzzyValue
     * @return
     */
    public List<Map<String, Object>> fuzzyQuery(String fieldName, String fuzzyValue) {
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery(fieldName, fuzzyValue);
        return query(queryBuilder);
    }

    /**
     * 通配符查询
     *
     * @param fieldName
     * @param query
     * @return
     */
    public List<Map<String, Object>> wildcardQuery(String fieldName, String query) {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery(fieldName, query);
        return query(queryBuilder);
    }

    /**
     * 短语查询
     *
     * @param fieldName
     * @param query
     * @return
     */
    public List<Map<String, Object>> matchPhraseQuery(String fieldName, String query) {
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(fieldName, query);
        return query(queryBuilder);
    }
}
