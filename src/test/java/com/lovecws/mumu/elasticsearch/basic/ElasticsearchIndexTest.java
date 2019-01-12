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
public class ElasticsearchIndexTest extends ElasticsearchBaseTest {

    public static final Logger log = Logger.getLogger(ElasticsearchIndexTest.class);
    public ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();

    @Test
    public void exists() {
        boolean exists = elasticsearchIndex.exists("gynetres");
        log.info(exists ? "索引存在" : "索引不存在");
    }

    @Test
    public void deleteIndex() {
        boolean deleteIndex = elasticsearchIndex.deleteIndex("gynetres_ignore");
        log.info(deleteIndex ? "索引删除操作成功" : "索引删除操作失败");
        if (deleteIndex) {
            createIndex();
        }
    }

    @Test
    public void closeIndex() {
        boolean deleteIndex = elasticsearchIndex.closeIndex("dns_domainparse_2018_06_02");
        log.info(deleteIndex ? "索引关闭操作成功" : "索引关闭操作失败");
        openIndex();
    }

    @Test
    public void openIndex() {
        boolean deleteIndex = elasticsearchIndex.openIndex("dns_domainparse_2018_06_02");
        log.info(deleteIndex ? "索引打开操作成功" : "索引打开操作失败");
    }

}
