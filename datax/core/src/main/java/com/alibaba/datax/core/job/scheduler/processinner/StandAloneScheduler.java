package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class StandAloneScheduler extends ProcessInnerScheduler{

    public StandAloneScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    protected boolean isJobKilling(Long jobId) {
    	Communication communication = LocalJobCommunicationManager.getInstance().getJobCommunication(jobId);
        if (communication.getState() != null && (
        		communication.getState().value() == State.KILLING.value()
        		|| communication.getState().value() == State.KILLED.value())) {
        	return true;
        }
        return false;
    }

}
