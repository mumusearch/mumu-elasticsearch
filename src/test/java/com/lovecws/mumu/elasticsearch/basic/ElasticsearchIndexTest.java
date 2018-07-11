package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引创建测试
 * @date 2018-06-03 13:42
 */
public class ElasticsearchIndexTest {

    public static final Logger log = Logger.getLogger(ElasticsearchIndexTest.class);
    public ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();

    @Test
    public void createIndex() {
        List<MappingEntity> mappings = new ArrayList<MappingEntity>();
        mappings.add(new MappingEntity("id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("key", "string", "not_analyzed"));
        mappings.add(new MappingEntity("dns_id", "string", "not_analyzed"));
        mappings.add(new MappingEntity("manager_name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("domain", "string", "not_analyzed"));
        mappings.add(new MappingEntity("ip_version", "long", "not_analyzed"));
        mappings.add(new MappingEntity("ip", "string", "not_analyzed"));
        mappings.add(new MappingEntity("visit_count", "long", "not_analyzed"));

        elasticsearchIndex.createIndex("dns_domainparse_2018_06_02", "dns_domainparse", "2018_06_02", mappings);
    }

    /**
     * 创建工业互联网索引
     */
    @Test
    public void createGyNetIndex() {
        List<MappingEntity> mappings = new ArrayList<MappingEntity>();
        mappings.add(new MappingEntity("task_id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("task_instance_id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("serial_number", "string", "not_analyzed"));
        mappings.add(new MappingEntity("vendor", "string", "not_analyzed"));
        mappings.add(new MappingEntity("primary_type.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("primary_type.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("ip", "string", "not_analyzed"));
        mappings.add(new MappingEntity("module_number", "string", "not_analyzed"));
        mappings.add(new MappingEntity("protocol", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("version", "string", "not_analyzed"));
        mappings.add(new MappingEntity("module", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.desc", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("res", "string", "not_analyzed"));
        mappings.add(new MappingEntity("scanner_level", "integer", "not_analyzed"));
        mappings.add(new MappingEntity("product_name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("port", "integer", "not_analyzed"));
        mappings.add(new MappingEntity("description", "string", "not_analyzed"));
        mappings.add(new MappingEntity("html", "string", "not_analyzed"));

        elasticsearchIndex.createIndex("gynetres", "gynet", "gynet_type", mappings);
    }

    @Test
    public void exists() {
        boolean exists = elasticsearchIndex.exists("dns_domainparse_2018_06_02");
        log.info(exists ? "索引存在" : "索引不存在");
    }

    @Test
    public void deleteIndex() {
        boolean deleteIndex = elasticsearchIndex.deleteIndex("dns_domainparse_2018_06_02");
        log.info(deleteIndex ? "索引删除操作成功" : "索引删除操作失败");
    }

    @Test
    public void closeIndex() {
        boolean deleteIndex = elasticsearchIndex.closeIndex("dns_domainparse_2018_06_02");
        log.info(deleteIndex ? "索引关闭操作成功" : "索引关闭操作失败");
    }

    @Test
    public void openIndex() {
        boolean deleteIndex = elasticsearchIndex.openIndex("dns_domainparse_2018_06_02");
        log.info(deleteIndex ? "索引打开操作成功" : "索引打开操作失败");
    }
}
