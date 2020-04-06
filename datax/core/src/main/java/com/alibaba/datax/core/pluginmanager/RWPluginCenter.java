package com.alibaba.datax.core.pluginmanager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.JarLoader;

public class RWPluginCenter {
	
	private static final String pluginTypeNameFormat = "plugin.%s.%s";
	private static final String cacheKey = "%s.%s.%s";

	private enum ContainerType {
		Job("Job"), Task("Task");
		private String type;

		private ContainerType(String type) {
			this.type = type;
		}

		public String value() {
			return type;
		}
	}
	/**
	 * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
	 * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
	 */
	private static Configuration pluginRegisterCenter;
	
	/**
	 * jarLoader的缓冲
	 */
	private static Map<String, JarLoader> jarLoaderCenter = new ConcurrentHashMap<>();

	//保证兼容性，暂不进行单例
//	private static Map<String, AbstractPlugin> instanceCenter = new ConcurrentHashMap<>();
	
	
	/**
	 * 设置pluginConfigs，方便后面插件来获取
	 *
	 * @param pluginConfigs
	 */
	public static void bind(Configuration pluginConfigs) {
		pluginRegisterCenter = pluginConfigs;
	}
	
	/**
	 * 加载JobPlugin，reader、writer都可能要加载
	 *
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractJobPlugin loadJobPlugin(PluginType pluginType, String pluginName) {
		Class<? extends AbstractPlugin> clazz = loadPluginClass(pluginType, pluginName, ContainerType.Job);

		try {
//			String plugName = generateCacheKey(pluginType, pluginName, ContainerType.Job.value());
//			if (instanceCenter.containsKey(plugName)) {
//				return (AbstractJobPlugin) instanceCenter.get(plugName);
//			}else {
				AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz.newInstance();
				jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
//				instanceCenter.put(plugName, jobPlugin);
				return jobPlugin;
//			}
		} catch (Exception e) {
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
					String.format("DataX找到plugin[%s]的Job配置.", pluginName), e);
		}
	}
	
	/**
	 * 加载taskPlugin，reader、writer都可能加载
	 *
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType, String pluginName, TaskGroupContext taskGroupContext) {
		Class<? extends AbstractPlugin> clazz = loadPluginClass(pluginType, pluginName, ContainerType.Task);

//		String plugName = generateCacheKey(pluginType, pluginName, ContainerType.Task.value());
//		if (instanceCenter.containsKey(plugName)) {
//			return (AbstractTaskPlugin) instanceCenter.get(plugName);
//		}else {
			try {
				AbstractTaskPlugin taskPlugin = 
						(AbstractTaskPlugin) clazz.getConstructor(TaskGroupContext.class).newInstance(taskGroupContext);
				taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
//				instanceCenter.put(plugName, taskPlugin);
				return taskPlugin;
			} catch (NoSuchMethodException e) {
				try {
					AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz.newInstance();
					taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
//					instanceCenter.put(plugName, taskPlugin);
					return taskPlugin;
				} catch (Exception sube) {
					throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
							String.format("DataX不能找plugin[%s]的Task配置.", pluginName), sube);
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
						String.format("DataX不能找plugin[%s]的Task配置.", pluginName), e);
			}
//		}

	}

	private static String generatePluginKey(PluginType pluginType, String pluginName) {
		return String.format(pluginTypeNameFormat, pluginType.toString(), pluginName);
	}
	
	private static String generateCacheKey(PluginType pluginRunType, String pluginName, String taskName) {
		return String.format(cacheKey, pluginRunType.toString(), pluginName, taskName);
	}
	
	public static Configuration getPluginConf(PluginType pluginType, String pluginName) {
		Configuration pluginConf = pluginRegisterCenter.getConfiguration(generatePluginKey(pluginType, pluginName));

		if (null == pluginConf) {
			throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
					String.format("DataX不能找到插件[%s]的配置.", pluginName));
		}

		return pluginConf;
	}
	
	private static Class<? extends AbstractPlugin> loadPluginClass(PluginType pluginType,
			String pluginName, ContainerType pluginRunType) {
		Configuration pluginConf = getPluginConf(pluginType, pluginName);
		JarLoader jarLoader = getJarLoader(pluginType, pluginName);
		ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
		try {
			classLoaderSwapper.setCurrentThreadClassLoader(jarLoader);
			return (Class<? extends AbstractPlugin>) jarLoader
					.loadClass(pluginConf.getString("class") + "$" + pluginRunType.value());
		} catch (Exception e) {
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			classLoaderSwapper.restoreCurrentThreadClassLoader();
		}
	}

	public static synchronized JarLoader getJarLoader(PluginType pluginType, String pluginName) {
		Configuration pluginConf = getPluginConf(pluginType, pluginName);

		JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType, pluginName));
		if (null == jarLoader) {
			String pluginPath = pluginConf.getString("path");
			if (StringUtils.isBlank(pluginPath)) {
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
						String.format("%s插件[%s]路径非法!", pluginType, pluginName));
			}
			jarLoader = new JarLoader(new String[] { pluginPath }, Thread.currentThread().getContextClassLoader());
			jarLoaderCenter.put(generatePluginKey(pluginType, pluginName), jarLoader);
		}

		return jarLoader;
	}
}
