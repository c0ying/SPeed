package com.alibaba.datax.core.util;

import com.alibaba.fastjson.JSON;

public class JsonUtil {

    public static <T> T parse(String json, Class<T> cls){
       return JSON.parseObject(json, cls);
    }

    public static String toJson(Object obj){
        return JSON.toJSONString(obj);
    }
}
