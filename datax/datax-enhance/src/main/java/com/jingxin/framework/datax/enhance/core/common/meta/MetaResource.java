package com.jingxin.framework.datax.enhance.core.common.meta;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigurationInitialize;
import com.alibaba.datax.core.util.CoreConfigurationUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.jingxin.framework.datax.enhance.core.job.JobControllerPanel;
import com.jingxin.framework.datax.enhance.core.statistics.communication.CommunicationManagerDelegateFactory;
import com.jingxin.framework.datax.enhance.core.statistics.communication.LocalTGCommunicationCleaner;

public class MetaResource {

	public static void init(Configuration configuration) {
		CommunicationManagerDelegateFactory.refJobComuniManager(configuration);
		CommunicationManagerDelegateFactory.refTGComuniManager(configuration);
		JobControllerPanel.newInstance();
		if (ExecuteMode.DISTRIBUTE != CoreConfigurationUtil.getExecuteMode(ConfigurationInitialize.CORE_CONFIGURATION)){
			//统计信息定时清除器
			new Thread(new LocalTGCommunicationCleaner()).start();
		}
	}
	
	public static void destory() {
	}

}
