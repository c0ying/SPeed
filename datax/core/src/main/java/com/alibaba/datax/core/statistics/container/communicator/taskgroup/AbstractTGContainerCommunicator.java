package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.TGCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.AbstractTGReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;

import java.util.List;
import java.util.Map;

/**
 * 该类是用于处理 taskGroupContainer 的 communication 的收集汇报的父类
 * 主要是 taskCommunicationMap 记录了 taskExecutor 的 communication 属性
 */
public abstract class AbstractTGContainerCommunicator extends AbstractContainerCommunicator {

    protected long jobId;

    protected int taskGroupId;
    
    protected TGCollector tgCollector;
    protected AbstractTGReporter tgReporter;

    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
    	tgCollector.registerTaskCommunication(configurationList);
    }

    @Override
    public final Communication collect() {
        return tgCollector.collectFromTask();
    }

    @Override
    public final State collectState() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :tgCollector.getCommunicationMap().values()) {
            communication.mergeStateFrom(taskCommunication);
        }

        return communication.getState();
    }

    public final Communication getTaskCommunication(Integer taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return tgCollector.getTaskCommunication(taskId);
    }

    public void reportTaskCommunication(Integer id, Communication communication){
        tgReporter.reportTaskCommunication(id, communication);
    }

    public Communication getTGCommunication(){
        return tgCollector.getTGCommunication();
    }

    public final Map<Integer, Communication> getTaskCommunicationMap() {
        return tgCollector.getCommunicationMap();
    }

    public TGCollector getCollector() {
        return tgCollector;
    }

    public void setCollector(TGCollector tgCollector) {
        this.tgCollector = tgCollector;
    }

    public AbstractTGReporter getReporter() {
        return tgReporter;
    }

    public void setReporter(AbstractTGReporter tgReporter) {
        this.tgReporter = tgReporter;
    }

    public abstract void resetTaskCommunication(Integer id);
}
