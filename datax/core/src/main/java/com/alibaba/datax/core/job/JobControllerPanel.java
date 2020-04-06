package com.alibaba.datax.core.job;

import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

public class JobControllerPanel {

	public static void killJob(long jobId) {
		LocalJobCommunicationManager.getInstance().getJobCommunication(jobId).setState(State.KILLING);
	}
}
