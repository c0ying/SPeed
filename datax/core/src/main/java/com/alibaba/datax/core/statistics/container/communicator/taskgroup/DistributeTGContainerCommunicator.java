package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.DistributeTGCollector;
import com.alibaba.datax.core.statistics.container.report.RedisTGReporter;

public class DistributeTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public DistributeTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        tgCollector = new DistributeTGCollector(this.jobId, this.taskGroupId, configuration);
        super.setReporter(new RedisTGReporter(this.jobId, this.taskGroupId, configuration));
    }

    @Override
    public void resetTaskCommunication(Integer id) {
        getReporter().reportTaskCommunication(id, new Communication());
    }

    @Override
    public void report(Communication communication) {
        super.getReporter().reportTGCommunication(communication);
    }
}
