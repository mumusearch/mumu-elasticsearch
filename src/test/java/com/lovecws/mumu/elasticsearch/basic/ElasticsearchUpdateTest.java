package com.lovecws.mumu.elasticsearch.basic;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: mumu-elasticsearch
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-04-25 11:06
 **/
public class ElasticsearchUpdateTest extends ElasticsearchBaseTest {

    public static final Logger log = Logger.getLogger(ElasticsearchUpdateTest.class);
    public ElasticsearchUpdate elasticsearchUpdate = new ElasticsearchUpdate();


    @Test
    public void upsert() {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put("url", "http://www.106666.com/favicon.ico");
        valueMap.put("key", "92ffc221d876cb906125fad3458594ac3");
        valueMap.put("create_time", new Date());
        valueMap.put("end_time", new Date());
        valueMap.put("counter", 1);
        int upsert = elasticsearchUpdate.upsert("jmr_ipunit", "jmr_ipunit_type", "92ffc221d876cb906125fad3458594ac3", valueMap);
        log.info(upsert);
    }


    @Test
    public void script() {
        Map<String, Object> scriptParams = new HashMap<String, Object>();
        scriptParams.put("url", "http://www.106666.com/favicon.ico1111111111111");
        int upsert = elasticsearchUpdate.script("jmr_ipunit", "jmr_ipunit_type", "92ffc221d876cb906125fad3458594ac2", "ctx._source.url = params.url", scriptParams);
        log.info(upsert);
    }


    @Test
    public void scriptUpsert() {
        //文档以存在 更新结束事件和计数数量
        Map<String, Object> scriptParams = new HashMap<String, Object>();
        scriptParams.put("end_time", new Date());
        scriptParams.put("counter", 1);

        //如果文档不存在 则添加文档
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put("url", "http://www.106666.com/favicon.ico");
        valueMap.put("key", "92ffc221d876cb906125fad3458594ac3");
        valueMap.put("create_time", new Date());
        valueMap.put("end_time", new Date());
        valueMap.put("counter", 1);
        int upsert = elasticsearchUpdate.scriptUpsert("jmr_ipunit", "jmr_ipunit_type",
                "92ffc221d876cb906125fad3458594ac3", "ctx._source.end_time = params.end_time;ctx._source.counter += params.counter",
                scriptParams, valueMap);
        log.info(upsert);
    }
}
