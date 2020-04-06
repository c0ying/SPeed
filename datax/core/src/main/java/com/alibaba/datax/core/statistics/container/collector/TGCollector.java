package com.alibaba.datax.core.statistics.container.collector;

import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;

public interface TGCollector {

	void registerTaskCommunication(List<Configuration> taskConfigurationList);
	
	Communication collectFromTask();
	
	Communication getTaskCommunication(Integer taskId);
	
	Map<Integer, Communication> getCommunicationMap();
}
