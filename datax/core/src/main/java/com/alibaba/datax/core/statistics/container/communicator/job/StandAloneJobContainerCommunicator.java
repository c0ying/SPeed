package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerJobCollector;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerJobReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandAloneJobContainerCommunicator extends AbstractJobContainerCommunicator {
    private static final Logger LOG = LoggerFactory
            .getLogger(StandAloneJobContainerCommunicator.class);
    

    public StandAloneJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        setCollector(new ProcessInnerJobCollector(configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        super.setReporter(new ProcessInnerJobReporter(this.getJobId()));
    }

    @Override
    public void report(Communication communication) {
        getReporter().reportJobCommunication(communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }
}
