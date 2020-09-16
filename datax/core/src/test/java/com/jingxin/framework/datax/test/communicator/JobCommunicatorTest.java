package com.jingxin.framework.datax.test.communicator;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalJobCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JobCommunicatorTest {

	@Before
	public void register() {
		LocalJobCommunicationManager.getInstance().registerJobCommunication(1l);
	}
	
	@Test
	public void registerTG() {
		List<Configuration> configurations = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			Configuration configuration = Configuration.newDefault();
			configuration.set("core.container.taskGroup.id", i);
			configurations.add(configuration);
		}
		
		communicator.registerCommunication(configurations);
		System.out.println(communicator.getTGCommunicationMap());
	}
	
	@Test
	public void collectState() {
		Communication jobCommunication = LocalJobCommunicationManager.getInstance().getJobCommunication(1l);
		jobCommunication.setState(State.SUBMITTING);
		System.out.println(communicator.collectState());
		System.out.println(LocalJobCommunicationManager.getInstance().getJobCommunication(1l));
	}
	@Test
	public void collectOnlyJobState() {
		Communication jobCommunication = LocalJobCommunicationManager.getInstance().getJobCommunication(1l);
		jobCommunication.setState(State.SUBMITTING);
//		System.out.println(communicator.collectState());
		System.out.println(LocalJobCommunicationManager.getInstance().getJobCommunication(1l));
	}
	
	
	public JobCommunicatorTest() {
		Configuration configuration = Configuration.newDefault();
		configuration.set("core.container.job.id", 1);
		communicator = new StandAloneJobContainerCommunicator(configuration); 
	}
	
	public StandAloneJobContainerCommunicator communicator;
}
