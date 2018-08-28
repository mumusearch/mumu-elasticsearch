package com.lovecws.mumu.elasticsearch.query;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;

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
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
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
            pool.removeClient(elasticsearchClient);
        }
        return null;
    }

    /**
     * scroll查询
     *
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param batchSize  批量大小
     * @return
     */
    public List<Map<String, Object>> scroll(String fieldName, Object fieldValue, int batchSize) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        String scrollId = null;
        int hits = batchSize;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(batchSize);
            if (fieldName != null && fieldValue != null) {
                searchRequestBuilder.setQuery(new TermQueryBuilder(fieldName, fieldValue));
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            long totalHits = searchResponse.getHits().getTotalHits();
            log.info("scrollId:" + scrollId);
            log.info("totalHits:" + searchResponse.getHits().getTotalHits());
            //scroll查询 当获取的数据量小于批处理数量 则退出scroll查询
            while (totalHits > 0 && hits == batchSize) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                hits = response.getHits().getHits().length;
                scrollId = response.getScrollId();
                for (SearchHit searchHit : response.getHits()) {
                    datas.add(searchHit.getSource());
                }
                log.info("hits:" + hits);
                log.info("scrollId:" + scrollId);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                elasticsearchClient.client().prepareClearScroll().addScrollId(scrollId).get();
            }
            pool.removeClient(elasticsearchClient);
        }
        return datas;
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
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        String scrollId = null;
        if (currentPage == 0) {
            currentPage = 1;
        }
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(pageSize);
            if (fieldName != null && fieldValue != null) {
                searchRequestBuilder.setQuery(new TermQueryBuilder(fieldName, fieldValue));
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            //获取第currentPage页的数据
            int current_page = 1;
            while (current_page <= currentPage) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                scrollId = response.getScrollId();
                if (current_page == currentPage) {
                    for (SearchHit searchHit : response.getHits()) {
                        datas.add(searchHit.getSource());
                        log.info(searchHit.getId());
                    }
                }
                current_page++;
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                elasticsearchClient.client().prepareClearScroll().addScrollId(scrollId).get();
            }
            pool.removeClient(elasticsearchClient);
        }
        return datas;
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

    public void agg() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("src_ip_point", "61.183.133.204");
        ValueCountAggregationBuilder countAggregationBuilder = AggregationBuilders.count("count").field("dst_ip_point");

        ElasticsearchClient elasticsearchClient = pool.buildClient();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames);
            searchRequestBuilder.addAggregation(countAggregationBuilder);
            searchRequestBuilder.setQuery(termQueryBuilder);

            SearchResponse searchResponse = searchRequestBuilder.get();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                System.out.println(JSON.toJSONString(hit.getSource()));
            }
            Terms terms = searchResponse.getAggregations().get("count");
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                System.out.println(JSON.toJSONString(bucket));
            }

        } catch (Exception e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
    }

    public void agg2() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("src_ip_point", "61.183.133.204");

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group").field("src_ip_point").size(1);
        ValueCountAggregationBuilder countAggregationBuilder = AggregationBuilders.count("count").field("dst_ip_point");
        termsAggregationBuilder.subAggregation(countAggregationBuilder);

        ElasticsearchClient elasticsearchClient = pool.buildClient();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames);
            searchRequestBuilder.addAggregation(termsAggregationBuilder);
            searchRequestBuilder.setQuery(termQueryBuilder);

            SearchResponse searchResponse = searchRequestBuilder.get();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                System.out.println(JSON.toJSONString(hit.getSource()));
            }
            Terms terms = searchResponse.getAggregations().get("group");
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                System.out.println(bucket.getDocCount());
                Aggregation count = bucket.getAggregations().get("count");
                System.out.println(count);
            }

        } catch (Exception e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
    }
}
