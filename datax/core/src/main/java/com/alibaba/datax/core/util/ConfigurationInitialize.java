package com.alibaba.datax.core.util;

import java.io.File;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;

public class ConfigurationInitialize {

	public static final Configuration CORE_CONFIGURATION = Configuration.from(new File(CoreConstant.DATAX_CONF_PATH));
    public static final Configuration PLUGIN_CONFIGURATION = ConfigParser.parseAllPluginConfig();
    
    public static void init() {
    	LoadUtil.bind(PLUGIN_CONFIGURATION);
    }
}
