package org.io.common.conf;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;

public class Configuration {
    private Object root;
    private String json;

    private Configuration(final String json){
        this.root = JSON.parse(json);
    }

    public Configuration from(String json){
        boolean isJson = JSON.isValid(json);
        if(isJson){
            return new Configuration(json);
        }else{
            throw new JSONException("配置信息错误. 因为您提供的配置信息不是合法的JSON格式");
        }
    }

}
