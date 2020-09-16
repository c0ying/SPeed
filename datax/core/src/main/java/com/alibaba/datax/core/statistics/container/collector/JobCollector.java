package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;
import java.util.Map;

/**
 * 采集者与汇报者都只能向自己归属的任务汇报数据或采集数据
 * 不能跨任务获取数据，所以JobCollector获取JobCommunication无需输入jobId
 */
public interface JobCollector {
	
	void registerJobCommunication();

	void registerTGCommunication(List<Configuration> taskGroupConfigurationList);
	
	Communication collectFromTaskGroup();
	
	Map<Integer, Communication> getTGCommunicationMap();
	
	Communication getTGCommunication(Integer taskGroupId);

	Communication getJobCommunication();
}
