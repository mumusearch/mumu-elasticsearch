package com.lovecws.mumu.elasticsearch.common;

import com.lovecws.mumu.elasticsearch.entity.MappingEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 映射
 * @date 2018-06-03 16:59
 */
public class ElasticsearchMapping {

    public static final Logger log = Logger.getLogger(ElasticsearchMapping.class);

    public static XContentBuilder mapping(String typeName, List<MappingEntity> mappings) {
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(typeName)
                    .startObject("_source")
                    .field("enabled", true)
                    .endObject()
                    .startObject("_all")
                    .field("enabled", false)
                    .endObject()
                    .startObject("_ttl")
                    .field("enabled", false)
                    .endObject()
                    .startObject("properties");
            for (MappingEntity mapping : mappings) {
                contentBuilder
                        .startObject(mapping.getFieldName())
                        .field("type", mapping.getFieldType())
                        .field("index", mapping.getFieldIndex())
                        .endObject();
            }
            contentBuilder.endObject().endObject().endObject();
            return contentBuilder;
        } catch (IOException e) {
            log.error(e);
            throw new IllegalArgumentException("mapping 映射错误");
        }
    }

    public static XContentBuilder content(Map<String, Object> valueMap) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            xContentBuilder.field(entry.getKey(), entry.getValue());
        }
        xContentBuilder.endObject();
        return xContentBuilder;
    }
}
