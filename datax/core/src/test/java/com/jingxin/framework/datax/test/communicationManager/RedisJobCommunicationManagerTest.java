package com.jingxin.framework.datax.test.communicationManager;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.redis.RedisJobCommunicationManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.stream.Collectors;

public class RedisJobCommunicationManagerTest {

    private RedisJobCommunicationManager redisJobCommunicationManager;

    @Before
    public void before(){
        Configuration core = Configuration.from(new File(Thread.currentThread().getContextClassLoader().getResource("conf/core.json").getFile()));
        redisJobCommunicationManager = RedisJobCommunicationManager.newInstance(core);
    }

    @Test
    public void register(){
        redisJobCommunicationManager.registerJobCommunication(1L);
        redisJobCommunicationManager.registerTaskGroupCommunication(1, 1, new Communication());
        redisJobCommunicationManager.registerTaskGroupCommunication(1, 2, new Communication());
    }

    @Test
    public void get(){
        System.out.println("=====job Communication=====");
        Communication jobCommunication = redisJobCommunicationManager.getJobCommunication(1);
        System.out.println(jobCommunication);
        Assert.assertNotNull("jobCommunication can not register", jobCommunication);
        System.out.println("=====all job id =====");
        String jobIdStr = redisJobCommunicationManager.getAllJobId().stream().map(j -> String.valueOf(j)).collect(Collectors.joining(","));
        System.out.println(jobIdStr);
        Assert.assertEquals("1", jobIdStr);
        System.out.println("=====taskGroup Communication=====");
        System.out.println(redisJobCommunicationManager.getTaskGroupCommunication(1, 1));
        System.out.println("=====taskGroup Map Communication=====");
        redisJobCommunicationManager.getTaskGroupCommunicationMap(1).forEach((k,v) -> System.out.println(v));
        System.out.println("=====taskGroupId Communication=====");
        System.out.println(redisJobCommunicationManager.getTaskGroupIdSet(1).stream().map(t -> String.valueOf(t)).collect(Collectors.joining(",")));
    }

    @Test
    public void clean(){
        redisJobCommunicationManager.clean(1L);
    }
}
