package com.jingxin.framework.datax.enhance.core.statistics.communication;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.JobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.communication.TGCommunicationManager;
import com.alibaba.datax.core.statistics.communication.redis.RedisJobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.redis.RedisTGCommunicationManager;
import com.alibaba.datax.core.util.CoreConfigurationUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;

public class CommunicationManagerDelegateFactory {

    private static JobCommunicationManager jobCommunicationManager;
    private static TGCommunicationManager tgCommunicationManager;

    public static synchronized JobCommunicationManager refJobComuniManager(Configuration core){
        if (ExecuteMode.DISTRIBUTE == CoreConfigurationUtil.getExecuteMode(core)){
            jobCommunicationManager = RedisJobCommunicationManager.newInstance(core);
            return jobCommunicationManager;
        }else{
            jobCommunicationManager = LocalJobCommunicationManager.getInstance();
            return jobCommunicationManager;
        }
    }

    public static synchronized TGCommunicationManager refTGComuniManager(Configuration core){
        if (ExecuteMode.DISTRIBUTE == CoreConfigurationUtil.getExecuteMode(core)){
            tgCommunicationManager = RedisTGCommunicationManager.newInstance(core);
            return tgCommunicationManager;
        }else{
            tgCommunicationManager = LocalTGCommunicationManager.getInstance();
            return tgCommunicationManager;
        }
    }

    public static JobCommunicationManager getJobCommunicationManager() {
        return jobCommunicationManager;
    }

    public static TGCommunicationManager getTgCommunicationManager() {
        return tgCommunicationManager;
    }
}
