package com.alibaba.datax.core.util;

import org.codehaus.groovy.control.CompilationFailedException;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transformer.TransformerErrorCode;

import groovy.lang.GroovyClassLoader;

public class GroovyJavaSupport {

	 public static Object initGroovyCode(String code) {
        GroovyClassLoader loader = new GroovyClassLoader(GroovyJavaSupport.class.getClassLoader());

        Class groovyClass;
        try {
            groovyClass = loader.parseClass(code);
        } catch (CompilationFailedException cfe) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, cfe);
        }

        try {
            Object t = groovyClass.newInstance();
//            if (!(t instanceof TaskGroupLifeCycle)) {
//                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, "datax bug! contact askdatax");
//            }
           return t;
        } catch (Throwable ex) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, ex);
        }
    }
}
