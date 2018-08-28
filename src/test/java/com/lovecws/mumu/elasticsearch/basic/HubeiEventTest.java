package com.lovecws.mumu.elasticsearch.basic;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author 甘亮
 * @groupdesc 湖北僵木蠕数据迁移到工业互联网es中
 * @note 2018/8/2 15:05
 */
public class HubeiEventTest {
    private static final Logger log = Logger.getLogger(ElasticsearchBulkTest.class);
    public ElasticsearchBulk elasticsearchBulk = new ElasticsearchBulk();
    public ElasticsearchIndex elasticsearchIndex = new ElasticsearchIndex();

    public void create_index(String jsonFile, String indexName, String aliasName, String indexType) {
        if (elasticsearchIndex.exists(indexName)) {
            System.out.println("索引已存在");
            return;
        }
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(jsonFile)));
            String readline = null;

            List<MappingEntity> mappings = new ArrayList<MappingEntity>();
            if ((readline = bufferedReader.readLine()) != null) {
                Map map = JSON.parseObject(readline, Map.class);
                Iterator iterator = map.keySet().iterator();
                while (iterator.hasNext()) {
                    Object next = iterator.next();
                    mappings.add(new MappingEntity(next.toString(), "string", "not_analyzed"));
                }
            }
            elasticsearchIndex.createIndex(indexName, aliasName, indexType, mappings);
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

    public void insert_data(String jsonFile, String indexName, String aliasName, String indexType) {
        if (!elasticsearchIndex.exists(indexName)) {
            System.out.println("索引不存在");
            return;
        }
        BufferedReader bufferedReader = null;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(jsonFile)));
            String readline = null;
            int current_index = 0;
            while ((readline = bufferedReader.readLine()) != null) {
                current_index++;
                Map map = JSON.parseObject(readline, Map.class);
                datas.add(map);
                if (current_index == 10000) {
                    elasticsearchBulk.bulk(indexName, indexType, datas);
                    current_index = 0;
                    datas.clear();
                }
            }
            if (current_index > 0) {
                elasticsearchBulk.bulk(indexName, indexType, datas);
            }
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

    @Test
    public void hubei_event_worm() {
        String path = "D:\\workspaceIDEA\\ipchecker\\event\\hubei\\20180802104810\\worm.txt";
        String indexName = "worm";
        String aliasName = "worm_alias";
        String indexType = "worm_type";
        create_index(path, indexName, aliasName, indexType);
        insert_data(path, indexName, aliasName, indexType);
    }

    @Test
    public void hubei_event_bot_or_trojan() {
        String path = "D:\\workspaceIDEA\\ipchecker\\event\\hubei\\20180802104810\\bot_or_trojan.txt";
        String indexName = "bot_or_trojan";
        String aliasName = "bot_or_trojan_alias";
        String indexType = "bot_or_trojan_type";
        create_index(path, indexName, aliasName, indexType);
        insert_data(path, indexName, aliasName, indexType);
    }
}
