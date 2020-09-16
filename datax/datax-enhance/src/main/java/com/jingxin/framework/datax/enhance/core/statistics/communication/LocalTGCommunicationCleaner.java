package com.jingxin.framework.datax.enhance.core.statistics.communication;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LocalTGCommunicationCleaner implements Runnable{

	@Override
	public void run() {
		Set<Long> jobIds = LocalJobCommunicationManager.getInstance().getAllJobId();
		for (Long jobId : jobIds) {
			Communication jobCommunication = LocalJobCommunicationManager.getInstance().getJobCommunication(jobId);
			if (jobCommunication.getState().value() >= State.KILLED.value()) {
				if(jobCommunication.getTimestamp() + TimeUnit.HOURS.toMillis(8) > System.currentTimeMillis()) {
					LocalJobCommunicationManager.getInstance().clean(jobId);
					LocalTGCommunicationManager.getInstance().clean(jobId);
				}
			}
		}
		try {
			Thread.sleep(TimeUnit.HOURS.toMillis(1));
		} catch (InterruptedException e) {}
	}

	public LocalTGCommunicationCleaner() {
		Thread communicationCleanThread = new Thread(this);
		communicationCleanThread.setDaemon(true);
		communicationCleanThread.setName("LocalTGCommunication-Cleaner");
		communicationCleanThread.start();
	}
}
