package com.jingxin.framework.datax.test.engine;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.junit.Before;
import org.junit.Test;

public class EngineTest {

    @Before
    public void before(){
    }

    @Test
    public void start(){
        Configuration task = ConfigParser.parse(Thread.currentThread().getContextClassLoader().getResource("job/job.json").getFile());
        task.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 2);
        Engine engine = new Engine();
        engine.start(task);
    }
}
