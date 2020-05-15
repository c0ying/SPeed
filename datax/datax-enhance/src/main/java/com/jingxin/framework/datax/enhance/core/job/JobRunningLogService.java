package com.jingxin.framework.datax.enhance.core.job;

import com.jingxin.framework.datax.enhance.core.utils.FileLogUtil;

public class JobRunningLogService {

    public String getRunningLog(String jobId){
        String dataxHome = System.getProperty("datax.home");
        String logFile = System.getProperty("datax.log",dataxHome+"/log/speed_log.log");
        return FileLogUtil.getJobRunningLog(logFile, jobId);
    }
}
