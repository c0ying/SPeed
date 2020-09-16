package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.JobCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.AbstractJobReporter;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.List;
import java.util.Map;

public abstract class AbstractJobContainerCommunicator extends AbstractContainerCommunicator {

    protected JobCollector jobCollector;
    protected AbstractJobReporter jobReporter;

    public AbstractJobContainerCommunicator(Configuration configuration) {
        super(configuration);
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

    @Override
    public void report(Communication communication) {
        getReporter().reportJobCommunication(communication);
    }

    /**
     *
     * @return
     */
    public Communication getJobCommunication() {
        return jobCollector.getJobCommunication();
    }

    public void registerJobCommunication() {
        jobCollector.registerJobCommunication();
    }

    /**
     *
     * @param taskGroupId
     * @return
     */
    public Communication getTGCommunication(Integer taskGroupId) {
        return jobCollector.getTGCommunication(taskGroupId);
    }

    public Map<Integer, Communication> getTGCommunicationMap() {
        return jobCollector.getTGCommunicationMap();
    }

    public JobCollector getCollector() {
        return jobCollector;
    }

    public void setCollector(JobCollector jobCollector) {
        this.jobCollector = jobCollector;
    }

    public AbstractJobReporter getReporter() {
        return jobReporter;
    }

    public void setReporter(AbstractJobReporter jobReporter) {
        this.jobReporter = jobReporter;
    }
}
