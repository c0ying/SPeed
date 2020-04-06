package com.jingxin.framework.datax.enhance.network.http.utils;

import com.alibaba.fastjson.JSON;

public class JsonTools {

    /**
     * Read t.
     *
     * @param <T>        the type parameter
     * @param jsonString the json string
     * @param claz       the claz
     * @return the t
     */
    public static  <T> T read(String jsonString, Class<T> claz) {
    	return JSON.parseObject(jsonString, claz);
    }

    /**
     * @param obj the obj
     * @return the string
     */
    public static String write(Object obj){
    	return JSON.toJSONString(obj);
    }
}