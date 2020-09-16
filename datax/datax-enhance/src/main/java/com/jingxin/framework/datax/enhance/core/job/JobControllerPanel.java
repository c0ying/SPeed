package com.jingxin.framework.datax.enhance.core.job;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.JobCommunicationManager;
import com.alibaba.datax.core.statistics.communication.TGCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.jingxin.framework.datax.enhance.core.statistics.communication.CommunicationManagerDelegateFactory;

import java.util.Map;

public class JobControllerPanel {

	private JobCommunicationManager jobCommunicationManager;
	private TGCommunicationManager tgCommunicationManager;

	public static JobControllerPanel instance;

	public JobControllerPanel(){
		this.jobCommunicationManager = CommunicationManagerDelegateFactory.getJobCommunicationManager();
		this.tgCommunicationManager = CommunicationManagerDelegateFactory.getTgCommunicationManager();
		if (this.jobCommunicationManager == null || this.tgCommunicationManager == null){
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "maybe jobCommunicationManager or jobCommunicationManager init fail");
		}
	}

	public void killJob(long jobId) {
		Communication jobCommunication = CommunicationManagerDelegateFactory.getJobCommunicationManager().getJobCommunication(jobId);
		jobCommunication.setState(State.KILLING);
		jobCommunicationManager.updateJobCommunication(jobId, jobCommunication);
		Map<Integer, Communication> taskGroupCommunicationMap = jobCommunicationManager.getTaskGroupCommunicationMap(jobId);
		taskGroupCommunicationMap.forEach((k,v)->{
			v.setState(State.KILLING);
			tgCommunicationManager.updateTaskGroupCommunication(jobId, k, v);
		});
	}

	public static synchronized JobControllerPanel newInstance(){
		if (instance == null){
			instance = new JobControllerPanel();
		}
		return instance;
	}
}
