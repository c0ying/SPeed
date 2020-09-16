package com.jingxin.framework.datax.test.communicationManager;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisTGCommunicationManager;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class RedisTGCommunicationManagerTest {

    Configuration core;
    RedisTGCommunicationManager redisTGCommunicationManager;

    @Before
    public void before(){
        core = Configuration.from(new File(Thread.currentThread().getContextClassLoader().getResource("conf/core.json").getFile()));
        redisTGCommunicationManager = RedisTGCommunicationManager.newInstance(core);
    }

    @Test
    public void registerTaskinTG(){
        redisTGCommunicationManager.register(1, 1, 1, new Communication());
    }
}
