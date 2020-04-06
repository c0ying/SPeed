package com.alibaba.datax.core.statistics.container.collector;

import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;

public interface JobCollector {
	
	void registerJobCommunication();

	void registerTGCommunication(List<Configuration> taskGroupConfigurationList);
	
	Communication collectFromTaskGroup();
	
	Map<Integer, Communication> getTGCommunicationMap();
	
	Communication getTGCommunication(Integer taskGroupId);
}
