package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerTGCollector;
import com.alibaba.datax.core.statistics.container.collector.TGCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

/**
 * 该类是用于处理 taskGroupContainer 的 communication 的收集汇报的父类
 * 主要是 taskCommunicationMap 记录了 taskExecutor 的 communication 属性
 */
public abstract class AbstractTGContainerCommunicator extends AbstractContainerCommunicator {

    protected long jobId;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;
    
    protected TGCollector tgCollector;

    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        this.jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        tgCollector = (new ProcessInnerTGCollector(this.jobId, this.taskGroupId));
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

    @Override
    public final Communication getCommunication(Integer taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return tgCollector.getTaskCommunication(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap() {
        return tgCollector.getCommunicationMap();
    }

}
