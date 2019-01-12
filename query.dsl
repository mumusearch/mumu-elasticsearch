1、判断字段是否存在
{
  "query": {
    "exists": {
      "field": "filemd5"
    }
  }
}

2、按照字段分组
{
  "size" : 0,
  "query" : {
    "term" : {
      "province_name" : {
        "value" : "广东",
        "boost" : 1.0
      }
    }
  },
  "aggregations" : {
    "fieldAggregation" : {
      "terms" : {
        "field" : "domain_name",
        "size" : 10,
        "min_doc_count" : 1,
        "shard_min_doc_count" : 0,
        "show_term_doc_count_error" : false,
        "order" : [
          {
            "_count" : "desc"
          },
          {
            "_term" : "asc"
          }
        ]
      }
    }
  }
}

3、按照时间分组统计
{
  "size" : 0,
  "query" : {
    "term" : {
      "province_name" : {
        "value" : "广东",
        "boost" : 1.0
      }
    }
  },
  "aggregations" : {
    "trendAggregation" : {
      "date_histogram" : {
        "field" : "event_time",
        "interval" : "1h",
        "offset" : 0,
        "order" : {
          "_key" : "desc"
        },
        "keyed" : false,
        "min_doc_count" : 0
      }
    }
  }
}