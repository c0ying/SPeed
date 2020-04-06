package com.alibaba.datax.core.statistics.container.communicator.job;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.collector.JobCollector;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerJobCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

public class StandAloneJobContainerCommunicator extends AbstractContainerCommunicator {
    private static final Logger LOG = LoggerFactory
            .getLogger(StandAloneJobContainerCommunicator.class);
    
    protected JobCollector jobCollector;

    public StandAloneJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        jobCollector = (new ProcessInnerJobCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        super.setReporter(new ProcessInnerReporter(this.getJobId()));
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
    	jobCollector.registerTGCommunication(configurationList);
    }

    @Override
    public Communication collect() {
        return jobCollector.collectFromTaskGroup();
    }

    @Override
    public State collectState() {
        return this.collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().reportJobCommunication(super.getJobId(), communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        return jobCollector.getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return jobCollector.getTGCommunicationMap();
    }
}
