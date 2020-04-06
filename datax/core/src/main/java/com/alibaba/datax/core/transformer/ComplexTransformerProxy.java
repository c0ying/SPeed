package com.alibaba.datax.core.transformer;

import java.util.Map;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.spi.ComplexTransformer;
import com.alibaba.datax.common.spi.Transformer;

/**
 * no comments.
 * Created by liqiang on 16/3/8.
 */
public class ComplexTransformerProxy extends ComplexTransformer {
    private Transformer realTransformer;

    public ComplexTransformerProxy(Transformer transformer) {
        setTransformerName(transformer.getTransformerName());
        this.realTransformer = transformer;
    }

    @Override
    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
        return this.realTransformer.evaluate(record, paras);
    }

    public Transformer getRealTransformer() {
        return realTransformer;
    }
}
