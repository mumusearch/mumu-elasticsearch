package com.lovecws.mumu.elasticsearch.entity;

import java.io.Serializable;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: mapping实体
 * @date 2018-06-03 16:58
 */
public class MappingEntity implements Serializable{

    private String fieldName;
    private String fieldType;
    private String fieldIndex;

    public MappingEntity(String fieldName, String fieldType, String fieldIndex) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldIndex = fieldIndex;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldIndex() {
        return fieldIndex;
    }

    public void setFieldIndex(String fieldIndex) {
        this.fieldIndex = fieldIndex;
    }
}
