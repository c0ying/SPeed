package com.jingxin.framework.datax.test.pluginmanager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.core.pluginmanager.RWPluginCenter;
import com.alibaba.datax.core.util.ConfigurationInitialize;


public class RWPluginCenterTest {
	
	@Before
	public void before() {
		ConfigurationInitialize.init();
	}

	@Test
	public void loadJobPlugin() {
		AbstractJobPlugin jobPlugin = RWPluginCenter.loadJobPlugin(PluginType.READER, "streamreader");
		Assert.assertNotNull(jobPlugin);
	}
	
	@Test
	public void loadTaskPlugin() {
		AbstractTaskPlugin taskPlugin = RWPluginCenter.loadTaskPlugin(PluginType.READER, "streamreader", null);
		Assert.assertNotNull(taskPlugin);
	}
}
