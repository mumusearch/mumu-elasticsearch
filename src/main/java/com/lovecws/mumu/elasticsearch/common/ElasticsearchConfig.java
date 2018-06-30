package com.lovecws.mumu.elasticsearch.common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 配置管理
 * @date 2018-06-03 11:20
 */
public class ElasticsearchConfig {

    public static final String CONFIG_FILE="config.properties";
    public static final Properties properties=new Properties();
    public static final ElasticsearchConfig config=new ElasticsearchConfig();
    public static final Logger log=Logger.getLogger(ElasticsearchConfig.class);

    private ElasticsearchConfig(){
        try {
            properties.load(ClassLoader.getSystemResourceAsStream(CONFIG_FILE));
            properties.list(System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key,String defaultValue){
        return properties.getProperty(key,defaultValue);
    }

    public static int getInteger(String key,int defaultValue){
        String value = getProperty(key, String.valueOf(defaultValue));
        return Integer.parseInt(value);
    }

    public static boolean getBoolean(String key,boolean defaultValue){
        String value = getProperty(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(value);
    }

    public static String[] getArray(String key,String regex){
        String value = properties.getProperty(key,null);
        if(value==null){
            return null;
        }
        return value.split(regex);
    }
}
