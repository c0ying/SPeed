package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.collector.DistributeJobCollector;
import com.alibaba.datax.core.statistics.container.report.RedisJobReporter;
import com.alibaba.datax.core.util.container.CoreConstant;

public class DistributeJobContainerCommunicator extends StandAloneJobContainerCommunicator {

    public DistributeJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        setCollector(new DistributeJobCollector(configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID), configuration));
        setReporter(new RedisJobReporter(this.getJobId(), configuration));
    }
}
