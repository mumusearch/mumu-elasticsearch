package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 甘亮
 * @Description: 测试基准
 * @date 2018/7/11 18:33
 */
public class ElasticsearchBaseTest {

    @BeforeClass
    public void before() {
        createIndex();
        createGyNetIndex();
    }

    public ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();

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
    public void createGyNetIndex() {
        List<MappingEntity> mappings = new ArrayList<MappingEntity>();
        mappings.add(new MappingEntity("id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("task_id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("task_instance_id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("create_time", "string", "not_analyzed"));

        mappings.add(new MappingEntity("serial_number", "string", "not_analyzed"));
        mappings.add(new MappingEntity("primary_type.name", "string", "not_analyzed"));//服务类型（iot、isc、web）
        mappings.add(new MappingEntity("primary_type.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("ip", "string", "not_analyzed"));//IP地址
        mappings.add(new MappingEntity("module_number", "string", "not_analyzed"));
        mappings.add(new MappingEntity("protocol", "string", "not_analyzed"));//传输层协议
        mappings.add(new MappingEntity("device_name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("version", "string", "not_analyzed"));//固件/软件版本
        mappings.add(new MappingEntity("module", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.desc", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device_type.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("scanner_level", "integer", "not_analyzed"));
        mappings.add(new MappingEntity("product_name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("port", "integer", "not_analyzed"));//端口号

        mappings.add(new MappingEntity("product", "string", "not_analyzed"));
        mappings.add(new MappingEntity("vendor", "string", "not_analyzed"));
        mappings.add(new MappingEntity("description", "string", "not_analyzed"));
        mappings.add(new MappingEntity("product_version", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.primary.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.primary.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.third.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.third.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.third.desc", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.secondary.namecn", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.secondary.name", "string", "not_analyzed"));
        mappings.add(new MappingEntity("device.secondary.desc", "string", "not_analyzed"));
        mappings.add(new MappingEntity("certificate", "string", "not_analyzed"));
        mappings.add(new MappingEntity("service", "string", "not_analyzed"));
        mappings.add(new MappingEntity("res", "string", "not_analyzed"));
        mappings.add(new MappingEntity("module_num", "string", "not_analyzed"));
        mappings.add(new MappingEntity("html", "string", "not_analyzed"));
        mappings.add(new MappingEntity("model", "string", "not_analyzed"));
        mappings.add(new MappingEntity("os", "string", "not_analyzed"));

        mappings.add(new MappingEntity("related_fields", "string", "not_analyzed"));//涉及领域
        mappings.add(new MappingEntity("province", "string", "not_analyzed"));
        mappings.add(new MappingEntity("city", "string", "not_analyzed"));
        mappings.add(new MappingEntity("detail_area", "string", "not_analyzed"));
        mappings.add(new MappingEntity("operator_name", "string", "not_analyzed"));//运营商名称
        mappings.add(new MappingEntity("access_name", "string", "not_analyzed"));//接入商名称
        mappings.add(new MappingEntity("ip_unit", "string", "not_analyzed"));//使用单位名称

        elasticsearchIndex.createIndex("gynetres", "gynet", "gynet_type", mappings);
    }
}
