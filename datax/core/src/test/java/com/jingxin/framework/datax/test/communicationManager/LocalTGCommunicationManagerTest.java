package com.jingxin.framework.datax.test.communicationManager;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalTGCommunicationManagerTest {

	@Before
	public void register() {
		List<Configuration> configurations = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			Configuration configuration = Configuration.newDefault();
			configuration.set("taskId", i);
			configurations.add(configuration);
		}
		tgContainerCommunicator.registerCommunication(configurations);
	}
	
	@Test
	public void collect() {
		Communication communication = tgContainerCommunicator.collect();
		System.out.println(communication);
	}
	
	@Test
	public void collectState() {
		System.out.println(tgContainerCommunicator.collectState());
	}
	
	public LocalTGCommunicationManagerTest() {
		Configuration configuration = Configuration.newDefault();
		configuration.set("core.container.job.id", 1);
		configuration.set("core.container.taskGroup.id", 1);
		tgContainerCommunicator = new StandaloneTGContainerCommunicator(configuration);
	}
	
	private StandaloneTGContainerCommunicator tgContainerCommunicator = null;
}
