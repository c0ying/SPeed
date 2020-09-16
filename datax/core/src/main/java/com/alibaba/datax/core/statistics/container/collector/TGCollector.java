package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;
import java.util.Map;

public interface TGCollector {

	void registerTaskCommunication(List<Configuration> taskConfigurationList);
	
	Communication collectFromTask();
	
	Communication getTaskCommunication(Integer taskId);

	Communication getTGCommunication();
	
	Map<Integer, Communication> getCommunicationMap();
}
