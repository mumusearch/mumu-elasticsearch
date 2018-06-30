package com.lovecws.mumu.elasticsearch.basic;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 文档添加
 * @date 2018-06-03 18:34
 */
public class ElasticsearchBulkTest {
    public ElasticsearchBulk elasticsearchBulk = new ElasticsearchBulk();

    @Test
    public void index() {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put("id", "1");
        valueMap.put("key", "baidu");
        valueMap.put("dns_id", "1");
        valueMap.put("manager_name", "baidu");
        valueMap.put("domain", "www.baidu.com");
        valueMap.put("ip_version", 1l);
        valueMap.put("ip", "192.168.0.25");
        valueMap.put("visit_count", 2l);
        elasticsearchBulk.index("dns_domainparse_2018_06_02", "2018_06_02", valueMap);
    }

    @Test
    public void bulk() {
        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10000; i++) {
            Map<String, Object> valueMap = new HashMap<String, Object>();
            valueMap.put("id", String.valueOf(i));
            valueMap.put("key", "baidu");
            valueMap.put("dns_id", "1");
            valueMap.put("manager_name", "baidu");
            valueMap.put("domain", "www.baidu.com");
            valueMap.put("ip_version", 1l);
            valueMap.put("ip", "192.168.0.25");
            valueMap.put("visit_count", 2l);
            values.add(valueMap);
        }
        elasticsearchBulk.bulk("dns_domainparse_2018_06_02", "2018_06_02", values);
    }
}
