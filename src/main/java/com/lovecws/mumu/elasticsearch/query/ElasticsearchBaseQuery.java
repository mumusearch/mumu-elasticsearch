package com.lovecws.mumu.elasticsearch.query;

import com.lovecws.mumu.elasticsearch.proxy.ElasticsearchThreadLocal;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author 甘亮
 * @Description: 基础查询
 * @date 2018/7/12 12:50
 */
public class ElasticsearchBaseQuery {

    public static final Logger log = Logger.getLogger(ElasticsearchBaseQuery.class);

    public String[] indexNames;
    public String typeName;
    public int pageCount;
    public int beginIndex;

    public ElasticsearchBaseQuery(String[] indexNames, String typeName) {
        this.indexNames = indexNames;
        this.typeName = typeName;
        if (indexNames == null || indexNames.length == 0) {
            throw new IllegalArgumentException("索引名称不能为空");
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
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        try {
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
            ElasticsearchThreadLocal.cleanup();
        }
        return null;
    }

    /**
     * scroll查询
     *
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param batchSize  批量大小
     * @return
     */
    public List<Map<String, Object>> scroll(String fieldName, Object fieldValue, int batchSize) {
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        String scrollId = null;
        int hits = batchSize;
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(batchSize);
            if (fieldName != null && fieldValue != null) {
                searchRequestBuilder.setQuery(new TermQueryBuilder(fieldName, fieldValue));
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            long totalHits = searchResponse.getHits().getTotalHits();
            log.info("scrollId:" + scrollId);
            log.info("totalHits:" + searchResponse.getHits().getTotalHits());
            //scroll查询 当获取的数据量小于批处理数量 则退出scroll查询
            while (totalHits > 0 && hits == batchSize) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                hits = response.getHits().getHits().length;
                scrollId = response.getScrollId();
                for (SearchHit searchHit : response.getHits()) {
                    datas.add(searchHit.getSource());
                }
                log.info("hits:" + hits);
                log.info("scrollId:" + scrollId);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                transportClient.prepareClearScroll().addScrollId(scrollId).get();
            }
            ElasticsearchThreadLocal.cleanup();
        }
        return datas;
    }

    /**
     * 滚屏分页查询
     *
     * @param queryBuilder 查询条件
     * @param currentPage  当前页数
     * @param pageSize     一页的大小
     * @return
     */
    public List<Map<String, Object>> getPageByScroll(QueryBuilder queryBuilder, int currentPage, int pageSize) {
        int scrollPageSize = 1000;
        TransportClient transportClient = ElasticsearchThreadLocal.get().client();
        String scrollId = null;
        if (currentPage == 0) {
            currentPage = 1;
        }
        List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
        try {
            //查询获取到scrollId
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexNames)
                    .setTypes(typeName)
                    .setScroll("5m")
                    .setFrom(0)
                    .setSearchType(SearchType.DEFAULT)
                    .setSize(scrollPageSize);
            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            scrollId = searchResponse.getScrollId();
            //获取第currentPage页的数据
            int current_page = 1;

            while (true) {
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scrollId(scrollId);
                searchScrollRequest.scroll("5m");
                SearchResponse response = transportClient.searchScroll(searchScrollRequest).get();
                scrollId = response.getScrollId();
                if (current_page * scrollPageSize >= currentPage * pageSize) {
                    int beginIndex = (currentPage - 1) * pageSize - (current_page - 1) * scrollPageSize;
                    for (SearchHit searchHit : response.getHits()) {
                        datas.add(searchHit.getSource());
                    }
                    datas = datas.subList(beginIndex, beginIndex + pageSize);
                    break;
                }
                current_page++;
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            //清除scrollId
            if (scrollId != null) {
                transportClient.prepareClearScroll().addScrollId(scrollId).get();
            }
            ElasticsearchThreadLocal.cleanup();
        }
        return datas;
    }

    /**
     * 根据事件类型 算出来需要的时间数据
     *
     * @param dateType         统计的时间类型：1、本日(day)，2、本周(week)，3、本月(month)，4、本年(year)，5、24小时(24hour),6、固定的年月(fixed)，7、nearmonth 近一个月
     * @param boolQueryBuilder boolQuery
     */
    public void setDateTypeQueryBuilder(String dateType, String dateField, BoolQueryBuilder boolQueryBuilder) {
        if (dateType == null || "".equals(dateType)) {
            return;
        }
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.setTime(new Date());
        //匹配时间
        switch (dateType.toLowerCase()) {
            case "nearday":
                calendar.add(Calendar.DAY_OF_MONTH, -1);
                calendar.add(Calendar.HOUR, 1);
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(DateUtils.truncate(calendar, Calendar.HOUR).getTime().getTime()));
                break;
            case "day":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.DAY_OF_MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "week":
                calendar.setFirstDayOfWeek(Calendar.MONDAY);
                int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
                if (dayWeek == 1) {
                    dayWeek = 8;
                }
                calendar.add(Calendar.DATE, calendar.getFirstDayOfWeek() - dayWeek);
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.DAY_OF_MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "month":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.MONTH));
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            //近一个月
            case "nearmonth":
                calendar.add(Calendar.MONTH, -1);//减去一个月
                boolQueryBuilder.must(QueryBuilders.rangeQuery(dateField).gte(calendar.getTime().getTime()));
                break;
            case "year":
                calendar.setTime(DateUtils.truncate(calendar.getTime(), Calendar.YEAR));
                boolQueryBuilder.must(QueryBuilders.rangeQuery("event_time").gte(calendar.getTime().getTime()));
                break;
            default:
                //固定的时间 如yyyy、yyyy-MM、yyyy-MM-dd
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy"}) != null) {
                    setDateTypeQueryBuilder("year", dateField, boolQueryBuilder);
                }
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy-MM", "yyyyMM", "yyyy_MM"}) != null) {
                    setDateTypeQueryBuilder("month", dateField, boolQueryBuilder);
                }
                if (org.apache.http.client.utils.DateUtils.parseDate(dateType, new String[]{"yyyy-MM-dd", "yyyyMMdd", "yyyy_MM_dd"}) != null) {
                    setDateTypeQueryBuilder("day", dateField, boolQueryBuilder);
                }
                break;
        }
    }
}
