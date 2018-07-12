package com.lovecws.mumu.elasticsearch.query;

import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchPool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 甘亮
 * @Description: 基础查询
 * @date 2018/7/12 12:50
 */
public class ElasticsearchBaseQuery {

    public static final Logger log = Logger.getLogger(ElasticsearchBaseQuery.class);
    public static final ElasticsearchPool pool = new ElasticsearchPool();

    public String[] indexNames;
    public String typeName;
    public int pageCount;
    public int beginIndex;

    public ElasticsearchBaseQuery(String[] indexNames, String typeName) {
        this.indexNames = indexNames;
        this.typeName = typeName;
        if (indexNames == null || indexNames.length == 0) {
            throw new IllegalArgumentException("索引名称[" + indexNames + "]不能为空");
        }
        this.pageCount = 5;
        this.beginIndex = 0;
    }

    /**
     * 基本查询
     *
     * @param queryBuilder
     * @return
     */
    public List<Map<String, Object>> query(QueryBuilder queryBuilder) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            SearchResponse searchResponse = transportClient.prepareSearch(indexNames)
                    .setSearchType(SearchType.DEFAULT)
                    .setTypes(typeName)
                    .setQuery(queryBuilder)
                    .setFrom(beginIndex)
                    .setSize(pageCount)
                    .get();
            log.info("查询总数:" + searchResponse.getHits().totalHits);
            List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : searchResponse.getHits()) {
                datas.add(searchHit.getSource());
            }
            return datas;
        } catch (Exception e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return null;
    }
}
