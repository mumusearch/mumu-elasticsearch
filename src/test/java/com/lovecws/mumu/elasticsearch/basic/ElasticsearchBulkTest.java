package com.lovecws.mumu.elasticsearch.basic;

import com.alibaba.fastjson.JSON;
import org.apache.http.client.utils.DateUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 文档添加
 * @date 2018-06-03 18:34
 */
public class ElasticsearchBulkTest extends ElasticsearchBaseTest {

    private static final Logger log = Logger.getLogger(ElasticsearchBulkTest.class);
    public ElasticsearchBulk elasticsearchBulk = new ElasticsearchBulk();

    @Test
    public void index() {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put("id", 1l);
        valueMap.put("task_id", 1l);
        valueMap.put("task_instance_id", "123");
        valueMap.put("create_time", DateUtils.formatDate(new Date()));

        valueMap.put("serial_number", "0x40636002");
        valueMap.put("primary_type.name", "ICS");
        valueMap.put("primary_type.namecn", "\\xe5\\xb7\\xa5\\xe6\\x8e\\xa7\\xe7\\xb3\\xbb\\xe7\\xbb\\x9f");
        valueMap.put("ip", "219.138.229.140");
        valueMap.put("module_number", "1766-L32BWA B/14.00");
        valueMap.put("protocol", "ethernetip-udp");
        valueMap.put("device_name", "MicroLogix 1400");
        valueMap.put("version", "14.00");
        valueMap.put("module", "1766-L32BWA");
        valueMap.put("device_type.name", "PLC");
        valueMap.put("device_type.desc", "Programmable Logic Controller");
        valueMap.put("device_type.namecn", "\\xe5\\x8f\\xaf\\xe7\\xbc\\x96\\xe7\\xa8\\x8b\\xe9\\x80\\xbb\\xe8\\xbe\\x91\\xe6\\x8e\\xa7\\xe5\\x88\\xb6\\xe5\\x99\\xa8");
        valueMap.put("scanner_level", 1);
        valueMap.put("product_name", "1766-L32BWA B/14.00");
        valueMap.put("port", 44818);

        valueMap.put("product", "MicroLogix 1400");
        valueMap.put("vendor", "Rockwell Automation/Allen-Bradley");
        valueMap.put("description", "Product name: 1766-L32BWA B/14.00\\r\\nVendor ID: Rockwell Automation/Allen-Bradley\\r\\nSerial number: 0x40636002\\r\\nDevice type: Programmable Logic Controller\\r\\nDevice IP: 219.138.229.140\\r\\n");
        valueMap.put("product_version", "14.00");
        valueMap.put("device.primary.namecn", "\\xe5\\xb7\\xa5\\xe6\\x8e\\xa7\\xe7\\xb3\\xbb\\xe7\\xbb\\x9f");
        valueMap.put("device.primary.name", "ICS");
        valueMap.put("device.third.namecn", "\\xe6\\x9c\\xaa\\xe7\\x9f\\xa5");
        valueMap.put("device.third.name", "unknown");
        valueMap.put("device.third.desc", "unknown");
        valueMap.put("device.secondary.namecn", "\\xe5\\x8f\\xaf\\xe7\\xbc\\x96\\xe7\\xa8\\x8b\\xe9\\x80\\xbb\\xe8\\xbe\\x91\\xe6\\x8e\\xa7\\xe5\\x88\\xb6\\xe5\\x99\\xa8");
        valueMap.put("device.secondary.name", "PLC");
        valueMap.put("device.secondary.desc", "Programmable Logic Controller");
        valueMap.put("certificate", "");
        valueMap.put("service", "ethernetip-udp");

        valueMap.put("res", "c\\x00;\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xc1\\xde\\xbe\\xd1\\x00\\x00\\x00\\x00\\x01\\x00\\x0c\\x005\\x00\\x01\\x00\\x00\\x02\\xaf\\x12\\xc0\\xa8\\x03\\xfa\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x0e\\x00Z\\x00\\x02\\x0ed\\x00\\x02`c@\\x131766-L32BWA B/14.00\\x00");
        valueMap.put("module_num", "1766-L32BWA B/14.00");
//        valueMap.put("html", "{}");
        valueMap.put("model", "1766-L32BWA");
        valueMap.put("os", "");
        elasticsearchBulk.index("gynetres", "gynet_type", valueMap);
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
            valueMap.put("ip_version", 1L);
            valueMap.put("ip", "192.168.0.25");
            valueMap.put("visit_count", 2L);
            values.add(valueMap);
        }
        elasticsearchBulk.bulk("dns_domainparse_2018_06_02", "2018_06_02", values);
    }

    @Test
    public void bulkTemplateIndex() {
        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> valueMap = new HashMap<String, Object>();
            valueMap.put("task_id", String.valueOf(i));
            valueMap.put("task_instance_id", "baidu");
            valueMap.put("create_time", "1");
            valueMap.put("edit_time", "1");
            values.add(valueMap);
        }
        elasticsearchBulk.bulk("gynetres_2018_06_02", "gynet_type", values);
    }

    @Test
    public void ipchecker() {
        BufferedReader bufferedReader = null;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(ElasticsearchBaseTest.class.getResourceAsStream("/ipunit_model.json")));
            String readline = null;
            while ((readline = bufferedReader.readLine()) != null) {
                Map map = JSON.parseObject(readline, Map.class);
                datas.add(map);
            }
            elasticsearchBulk.bulk("ipchecker_ipunit", "ipchecker_type", datas);
        } catch (IOException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
            }
        }
    }
}
