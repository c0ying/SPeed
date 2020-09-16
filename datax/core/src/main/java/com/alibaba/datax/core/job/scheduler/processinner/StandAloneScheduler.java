package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.communicator.job.AbstractJobContainerCommunicator;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class StandAloneScheduler extends ProcessInnerScheduler{

    public StandAloneScheduler(AbstractJobContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    protected boolean isJobKilling(Long jobId) {
    	Communication communication = jobContainerCommunicator.getJobCommunication();
        if (communication.getState() != null && (
        		communication.getState().value() == State.KILLING.value()
        		|| communication.getState().value() == State.KILLED.value())) {
        	return true;
        }
        return false;
    }

}
