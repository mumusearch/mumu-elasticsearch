package com.lovecws.mumu.elasticsearch.aggregation;

import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.client.utils.DateUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.joda.time.DateTime;

import java.util.*;

/**
 * @program: mumu-elasticsearch
 * @description: es基本统计
 * @author: 甘亮
 * @create: 2019-01-12 19:17
 **/
public class ElasticsearchBaseAggregation {

    public static final Logger log = Logger.getLogger(ElasticsearchBaseAggregation.class);

    /**
     * 获取日期DateHistogramInterval
     *
     * @param dateType 时间类型
     * @return DateHistogramInterval
     */
    public DateHistogramInterval getDateHistogramInterval(String dateType) {
        DateHistogramInterval dateHistogramInterval = DateHistogramInterval.YEAR;
        if ("nearday".equalsIgnoreCase(dateType) || "day".equalsIgnoreCase(dateType)) {
            dateHistogramInterval = DateHistogramInterval.HOUR;
        } else if ("week".equalsIgnoreCase(dateType) || "month".equalsIgnoreCase(dateType) || "nearmonth".equalsIgnoreCase(dateType)) {
            dateHistogramInterval = DateHistogramInterval.DAY;
        } else if ("year".equalsIgnoreCase(dateType)) {
            dateHistogramInterval = DateHistogramInterval.MONTH;
        } else {
            //固定的时间 如yyyy、yyyy-MM、yyyy-MM-dd
            if (dateType != null) {
                if (DateUtils.parseDate(dateType, new String[]{"yyyy"}) != null) {
                    dateHistogramInterval = getDateHistogramInterval("year");
                }
                if (DateUtils.parseDate(dateType, new String[]{"yyyy-MM", "yyyyMM", "yyyy_MM"}) != null) {
                    dateHistogramInterval = getDateHistogramInterval("month");
                }
                if (DateUtils.parseDate(dateType, new String[]{"yyyy-MM-dd", "yyyyMMdd", "yyyy_MM_dd"}) != null) {
                    dateHistogramInterval = getDateHistogramInterval("day");
                }
            }
        }
        return dateHistogramInterval;
    }

    /**
     * 获取日期DateHistogram的key
     *
     * @param dateType 日期类型
     * @param bucket   桶数据
     * @return 时间key
     */
    public String getDateHistogramKey(String dateType, Histogram.Bucket bucket) {
        Date date = ((DateTime) bucket.getKey()).toDate();
        String key = "";
        if ("nearday".equalsIgnoreCase(dateType) || "day".equalsIgnoreCase(dateType)) {
            key = DateFormatUtils.format(date, "HH:00");
        } else if ("week".equalsIgnoreCase(dateType) || "month".equalsIgnoreCase(dateType) || "nearmonth".equalsIgnoreCase(dateType)) {
            key = DateFormatUtils.format(date, "yyyy-MM-dd");
        } else if ("year".equalsIgnoreCase(dateType)) {
            key = DateFormatUtils.format(date, "yyyy-MM");
        } else {
            //固定的时间 如yyyy、yyyy-MM、yyyy-MM-dd
            if (dateType != null) {
                if (DateUtils.parseDate(dateType, new String[]{"yyyy"}) != null) {
                    key = getDateHistogramKey("year", bucket);
                }
                if (DateUtils.parseDate(dateType, new String[]{"yyyy-MM", "yyyyMM", "yyyy_MM"}) != null) {
                    key = getDateHistogramKey("month", bucket);
                }
                if (DateUtils.parseDate(dateType, new String[]{"yyyy-MM-dd", "yyyyMMdd", "yyyy_MM_dd"}) != null) {
                    key = getDateHistogramKey("day", bucket);
                }
            }
        }
        return key;
    }

    /**
     * 按照字段进行分组统计分析
     *
     * @param indexName    索引名称
     * @param queryBuilder 查询条件
     * @param groupField   分组字段
     * @param count        统计的数据量
     * @return
     */
    public List<Map<String, Object>> groupByTermField(String indexName, QueryBuilder queryBuilder, String groupField, int count) {
        return groupByTermCardinalityField(indexName, queryBuilder, groupField, null, count);
    }

    /**
     * 按照字段先去重在进行统计分析
     *
     * @param indexName        索引名称
     * @param queryBuilder     查询条件
     * @param groupField       分组字段
     * @param cardinalityField cardinality去重字段
     * @param count            统计的数据量
     * @return
     */
    public List<Map<String, Object>> groupByTermCardinalityField(String indexName, QueryBuilder queryBuilder, String groupField, String cardinalityField, int count) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch();
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setSize(0);
            //按照事件子类型进行分布统计
            TermsAggregationBuilder fieldAggregation = AggregationBuilders
                    .terms("fieldAggregation")
                    .field(groupField)
                    .size(count);
            if (StringUtils.isNotEmpty(cardinalityField)) {
                //按照去重字段排序
                fieldAggregation.order(Terms.Order.aggregation("cardinalityAggregation", false));
                fieldAggregation.subAggregation(AggregationBuilders.cardinality("cardinalityAggregation").field(cardinalityField));
            } else {
                fieldAggregation.order(Terms.Order.count(false));
            }
            searchRequestBuilder.addAggregation(fieldAggregation);
            //查询
            SearchResponse searchResponse = searchRequestBuilder.get();
            Terms aggregationTerms = searchResponse.getAggregations().get("fieldAggregation");
            for (Terms.Bucket bucket : aggregationTerms.getBuckets()) {
                Map<String, Object> operatorMap = new HashMap<>();
                operatorMap.put("key", bucket.getKey().toString());
                operatorMap.put("docCount", bucket.getDocCount());
                if (StringUtils.isNotEmpty(cardinalityField)) {
                    InternalCardinality cardinalityAggregation = bucket.getAggregations().get("cardinalityAggregation");
                    operatorMap.put("cardinalityCount", cardinalityAggregation.getValue());
                }
                results.add(operatorMap);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return results;
    }


    /**
     * 按照字段进行统计分析
     *
     * @param indexName    索引名称
     * @param queryBuilder 查询条件
     * @param dateType     时间维度统计
     * @param dateField    时间分组字段
     * @return
     */
    public List<Map<String, Object>> groupByDateHistogramField(String indexName, QueryBuilder queryBuilder, String dateType, String dateField) {
        return groupByDateHistogramCardinalityField(indexName, queryBuilder, dateType, dateField, null);
    }

    /**
     * 按照字段先去重在进行统计分析
     *
     * @param indexName        索引名称
     * @param queryBuilder     查询条件
     * @param dateType         查询条件
     * @param dateField        时间分组字段
     * @param cardinalityField cardinality去重字段
     * @return
     */
    public List<Map<String, Object>> groupByDateHistogramCardinalityField(String indexName, QueryBuilder queryBuilder, String dateType, String dateField, String cardinalityField) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);
            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            searchRequestBuilder.setSize(0);

            //按照事件子类型进行分布统计
            DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders.
                    dateHistogram("trendAggregation")
                    .field(dateField)
                    .dateHistogramInterval(getDateHistogramInterval(dateType))
                    .order(Histogram.Order.KEY_DESC);
            //按照时间来统计趋势后 按照字段去重
            if (cardinalityField != null && !"".equals(cardinalityField)) {
                dateHistogramAggregationBuilder.subAggregation(AggregationBuilders.cardinality("cardinalityAggregation").field(cardinalityField));
            }
            searchRequestBuilder.addAggregation(dateHistogramAggregationBuilder);
            //查询
            SearchResponse searchResponse = searchRequestBuilder.get();
            Histogram histogram = searchResponse.getAggregations().get("trendAggregation");


            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                Map<String, Object> operatorMap = new HashMap<>();
                operatorMap.put("key", getDateHistogramKey(dateType, bucket));
                operatorMap.put("docCount", bucket.getDocCount());
                if (cardinalityField != null && !"".equals(cardinalityField)) {
                    InternalCardinality cardinalityAggregation = bucket.getAggregations().get("cardinalityAggregation");
                    operatorMap.put("cardinalityCount", cardinalityAggregation.getValue());
                }
                results.add(operatorMap);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return results;
    }

    /**
     * 按照字段统计数量
     *
     * @param indexName    索引名称
     * @param queryBuilder 查询条件
     * @param countField   统计字段
     * @return
     */
    public long countByField(String indexName, QueryBuilder queryBuilder, String countField) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setSize(0);

            searchRequestBuilder.addAggregation(AggregationBuilders.count("countAggregation").field(countField));
            InternalValueCount count = searchRequestBuilder.get().getAggregations().get("countAggregation");
            return count.getValue();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return 0L;
    }

    /**
     * 计算去重数量
     *
     * @param indexName        索引名称
     * @param queryBuilder     查询条件
     * @param cardinalityField 去重统计字段
     * @return
     */
    public long countByCardinalityField(String indexName, QueryBuilder queryBuilder, String cardinalityField) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setSize(0);

            searchRequestBuilder.addAggregation(AggregationBuilders.cardinality("countAggregation").field(cardinalityField));
            InternalCardinality internalCardinality = searchRequestBuilder.get().getAggregations().get("countAggregation");
            return internalCardinality.getValue();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return 0L;

    }

    /**
     * 按照${groupField}进行分组，在按照${valueField}分组取第一条数据
     *
     * @param indexName    索引名称
     * @param queryBuilder 查询条件
     * @param groupField   分组字段
     * @param valueField   值字段
     * @param pageSize     统计的数据量
     * @return
     */
    public List<Map<String, Object>> groupByTermFieldValue(String indexName, QueryBuilder queryBuilder, String groupField, String valueField, int currentPage, int pageSize) {
        if (currentPage < 1) currentPage = 1;
        int count = currentPage * pageSize;

        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setSize(0);

            //按照事件子类型进行分布统计
            TermsAggregationBuilder fieldAggregation = AggregationBuilders
                    .terms("fieldAggregation")
                    .field(groupField)
                    .order(Terms.Order.count(false))
                    .size(count)
                    .subAggregation(AggregationBuilders.terms("termAggregation").field(valueField).size(1));
            //fieldAggregation.collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);//广度优先算法
            //fieldAggregation.executionHint("map");
            searchRequestBuilder.addAggregation(fieldAggregation);
            Terms aggregationTerms = searchRequestBuilder.get().getAggregations().get("fieldAggregation");

            //获取统计结果
            List<Map<String, Object>> counterList = new ArrayList<>();

            for (Terms.Bucket bucket : aggregationTerms.getBuckets()) {
                Terms fieldAggregationTerms = bucket.getAggregations().get("termAggregation");

                Map<String, Object> counterMap = new HashMap<>();
                counterMap.put("key", bucket.getKey());
                counterMap.put("docCount", bucket.getDocCount());

                Terms.Bucket subBucket = fieldAggregationTerms.getBuckets().get(0);
                counterMap.put("value", subBucket.getKey().toString());
                counterList.add(counterMap);
            }
            if (counterList.size() <= (currentPage - 1) * pageSize) {
                return new ArrayList<>();
            }
            return counterList.subList((currentPage - 1) * pageSize, counterList.size());
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            ElasticsearchThreadLocal.cleanup();
        }
        return new ArrayList<>();
    }

}
