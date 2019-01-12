package com.lovecws.mumu.elasticsearch.query;

import com.lovecws.mumu.elasticsearch.basic.ElasticsearchBaseTest;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @author 甘亮
 * @Description: 查询测试
 * @date 2018/7/11 17:44
 */
public class ElasticsearchQueryTest extends ElasticsearchBaseTest {

    private static final Logger log = Logger.getLogger(ElasticsearchQueryTest.class);

    ElasticsearchQuery elasticsearchQuery = new ElasticsearchQuery(new String[]{"guangdong_jmr"}, "guangdong_jmr_type");

    @Test
    public void queryById() {
        Map<String, Object> stringObjectMap = elasticsearchQuery.queryById("88f5da6b767fedcec43bc1976bcb83fa");
        log.info(stringObjectMap);
    }

    @Test
    public void scroll() {
        List<Map<String, Object>> mapList = elasticsearchQuery.scroll(null, null, 5000);
        printlnlist(mapList);
    }

    @Test
    public void getPageByScroll() {
        List<Map<String, Object>> mapList = elasticsearchQuery.getPageByScroll(null, null, 1040, 10);
        printlnlist(mapList);
    }

    @Test
    public void matchAllQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.matchAllQuery();
        printlnlist(mapList);
    }

    @Test
    public void idsQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.idsQuery("a26715312de04477ac3db6ba6e961c28");
        printlnlist(mapList);
    }

    @Test
    public void termQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.termQuery("src_ip_area", "湖");
        printlnlist(mapList);
    }

    @Test
    public void termsQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.termsQuery("product.vendorcn", "华", "海");
        printlnlist(mapList);
    }

    @Test
    public void commonTermsQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.commonTermsQuery("product.vendorcn", "海 华");
        printlnlist(mapList);
    }

    @Test
    public void matchQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.matchQuery("src_ip_area", "湖");
        printlnlist(mapList);
    }

    @Test
    public void matchBooleanQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.matchBooleanQuery(
                "product.vendorcn", "海康威视", "product.vendorcn", "海思");
        printlnlist(mapList);
    }

    @Test
    public void prefixQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.prefixQuery("product.vendorcn", "海");
        printlnlist(mapList);
    }

    @Test
    public void fuzzyQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.fuzzyQuery("product.vendorcn", "海");
        printlnlist(mapList);
    }

    @Test
    public void wildcardQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.wildcardQuery("product.vendorcn", "海*");
        printlnlist(mapList);
    }

    @Test
    public void matchPhraseQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.matchPhraseQuery("product.vendorcn", "海康威视");
        printlnlist(mapList);
    }
}
