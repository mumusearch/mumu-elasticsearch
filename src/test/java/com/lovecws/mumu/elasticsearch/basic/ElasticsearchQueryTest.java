package com.lovecws.mumu.elasticsearch.basic;

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

    ElasticsearchQuery elasticsearchQuery = new ElasticsearchQuery();

    @Test
    public void queryById() {
        Map<String, Object> stringObjectMap = elasticsearchQuery.queryById("gynetres",
                "gynet_type",
                "a8dd67022f5a47068394a8b8ea3d9864");
        log.info(stringObjectMap);
    }

    @Test
    public void termQuery() {
        List<Map<String, Object>> mapList = elasticsearchQuery.termQuery("gynetres",
                "gynet_type",
                "id",
                "1");
        for (Map map : mapList) {
            log.info(map);
        }
    }

    @Test
    public void scroll() {
        List<Map<String, Object>> mapList = elasticsearchQuery.scroll("gynetres",
                "gynet_type",
                null,
                null,
                100);
        for (Map map : mapList) {
            log.info(map);
        }
    }
}
