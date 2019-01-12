package com.lovecws.mumu.elasticsearch.aggregation;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.elasticsearch.query.ElasticsearchBaseQuery;
import junit.framework.TestCase;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;

/**
 * @program: mumu-elasticsearch
 * @description: 统计测试
 * @author: 甘亮
 * @create: 2019-01-12 21:28
 **/
public class ElasticsearchAggregationTest extends TestCase {

    private ElasticsearchAggregation elasticsearchAggregation = new ElasticsearchAggregation();
    private ElasticsearchBaseQuery elasticsearchBaseQuery = new ElasticsearchBaseQuery(new String[]{"guangdong_jmr"}, null);

    public void testGroupByDomainName() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("province_name", "广东");
        List<Map<String, Object>> mapList = elasticsearchAggregation.groupByTermField("guangdong_jmr", termQueryBuilder, "domain_name", 10);
        for (Map<String, Object> map : mapList) {
            System.out.println(JSON.toJSONString(map));
        }
    }

    public void testGroupByProtocol() {
        List<Map<String, Object>> mapList = elasticsearchAggregation.groupByTermField("guangdong_jmr", null, "protocol", 10);
        for (Map<String, Object> map : mapList) {
            System.out.println(JSON.toJSONString(map));
        }
    }

    public void testGroupByEventTime() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("province_name", "广东"));
        elasticsearchBaseQuery.setDateTypeQueryBuilder("month", "event_time", boolQueryBuilder);
        List<Map<String, Object>> mapList = elasticsearchAggregation.groupByDateHistogramField("guangdong_jmr", boolQueryBuilder, "month", "event_time");
        for (Map<String, Object> map : mapList) {
            System.out.println(JSON.toJSONString(map));
        }
    }
}
