package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerTGCollector;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerTGReporter;

public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public StandaloneTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        tgCollector = (new ProcessInnerTGCollector(this.jobId, this.taskGroupId));
        setReporter(new ProcessInnerTGReporter(this.jobId, this.taskGroupId));
    }

    @Override
    public void resetTaskCommunication(Integer id) {
        LocalTGCommunicationManager.getInstance().getTaskCommunicationMap(jobId, taskGroupId).put(id, new Communication());
    }

    @Override
    public void report(Communication communication) {
        getReporter().reportTGCommunication(communication);
    }

}
