package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引创建测试
 * @date 2018-06-03 13:42
 */
public class ElasticsearchTemplateTest extends ElasticsearchBaseTest {

    public static final Logger log = Logger.getLogger(ElasticsearchTemplateTest.class);
    public ElasticsearchTemplate elasticsearchTemplate = new ElasticsearchTemplate();

    @Test
    public void createTemplate() {
        List<MappingEntity> mappings = new ArrayList<MappingEntity>();
        mappings.add(new MappingEntity("task_id", "long", "not_analyzed"));
        mappings.add(new MappingEntity("task_instance_id", "string", "not_analyzed"));
        mappings.add(new MappingEntity("create_time", "string", "not_analyzed"));

        elasticsearchTemplate.createTemplate("dns_*_domaininfo", "domaininfo", "domaininfo", mappings);
    }

    @Test
    public void getTemplate() {
        List<IndexTemplateMetaData> templates = elasticsearchTemplate.getTemplate("gynetres_*");
        for (IndexTemplateMetaData indexTemplateMetaData : templates) {
            System.out.println(indexTemplateMetaData.getName());
        }
    }

}
