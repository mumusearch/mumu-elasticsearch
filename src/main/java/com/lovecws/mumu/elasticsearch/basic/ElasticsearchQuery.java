package com.lovecws.mumu.elasticsearch.basic;

import com.lovecws.mumu.elasticsearch.client.ElasticsearchClient;
import com.lovecws.mumu.elasticsearch.client.ElasticsearchPool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 查询
 * @date 2018-06-07 20:04
 */
public class ElasticsearchQuery {

    public static final Logger log = Logger.getLogger(ElasticsearchQuery.class);
    public static final ElasticsearchPool pool = new ElasticsearchPool();

    /**
     * get查询 使用get查询会很快的查询到数据  直接对id进行hash取模分片数量 直接定位到相应的分片上 然后直接到对应的分片的服务器上查询数据即可
     * 这种方式非常依赖于分片的routing ，如果插入数据的时候指定不同的routing 则使用get查询不到数据
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param id        主键
     * @return
     */
    public Map<String, Object> get(String indexName, String typeName, String id) {
        if (indexName == null || "".equals(indexName)) {
            return null;
        }
        if (typeName == null || "".equals(typeName)) {
            return null;
        }
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            GetResponse getResponse = transportClient.get(new GetRequest(indexName, typeName, id)).get();
            return getResponse.getSource();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
        } finally {
            pool.removeClient(elasticsearchClient);
        }
        return null;
    }

    /**
     * term字段查询
     *
     * @param indexName
     * @param typeName
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public List<Map<String, Object>> termQuery(String indexName, String typeName, String fieldName, Object fieldValue) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            TermQueryBuilder termQueryBuilder = new TermQueryBuilder(fieldName, fieldValue);
            SearchResponse searchResponse = transportClient.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT)
                    .setTypes(typeName)
                    .setQuery(termQueryBuilder)
                    .setFrom(0)
                    .setSize(10)
                    .get();
            List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : searchResponse.getHits()) {
                datas.add(searchHit.getSource());
            }
            return datas;
        } finally {
            pool.removeClient(elasticsearchClient);
        }
    }

    /**
     * scroll查询
     *
     * @param indexName  索引名称
     * @param typeName   类型名称
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param batchSize  批量大小
     * @return
     */
    public List<Map<String, Object>> scroll(String indexName, String typeName, String fieldName, Object fieldValue, int batchSize) {
        ElasticsearchClient elasticsearchClient = pool.buildClient();
        String scrollId = null;
        long totalHits = 0;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            TransportClient transportClient = elasticsearchClient.client();
            //查询获取到scrollId
            SearchResponse searchResponse = transportClient.prepareSearch(indexName)
                    .setTypes(typeName)
                    .setScroll("1m")
                    .setSearchType(SearchType.DEFAULT)
                    .setQuery(new TermQueryBuilder(fieldName, fieldValue))
                    .setSize(batchSize)
                    .get();
            scrollId = searchResponse.getScrollId();
            //scroll查询 当获取的数据量小于批处理数量 则退出scroll查询
            while (totalHits < batchSize) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                totalHits = response.getHits().totalHits();
                scrollId = response.getScrollId();
                for (SearchHit searchHit : searchResponse.getHits()) {
                    datas.add(searchHit.getSource());
                }
            }
        } catch (InterruptedException e) {
            log.error(e);
        } catch (ExecutionException e) {
            log.error(e);
        } finally {
            //清除scrollId
            elasticsearchClient.client().prepareClearScroll().addScrollId(scrollId).get();
            pool.removeClient(elasticsearchClient);
        }
        return datas;
    }
}
