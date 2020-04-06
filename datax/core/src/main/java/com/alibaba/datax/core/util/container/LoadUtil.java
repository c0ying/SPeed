package com.alibaba.datax.core.util.container;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.pluginmanager.RWPluginCenter;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Job和Task两种运行时（加载的类不同）
 */
public class LoadUtil {

	private LoadUtil() {
	}


	/**
	 * 设置pluginConfigs，方便后面插件来获取
	 *
	 * @param pluginConfigs
	 */
	public static void bind(Configuration pluginConfigs) {
		RWPluginCenter.bind(pluginConfigs);
	}

	/**
	 * 加载JobPlugin，reader、writer都可能要加载
	 *
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractJobPlugin loadJobPlugin(PluginType pluginType, String pluginName) {
		return RWPluginCenter.loadJobPlugin(pluginType, pluginName);
	}

	/**
	 * 加载taskPlugin，reader、writer都可能加载
	 *
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType, String pluginName, TaskGroupContext taskGroupContext) {
		return RWPluginCenter.loadTaskPlugin(pluginType, pluginName, taskGroupContext);

	}

	/**
	 * 根据插件类型、名字和执行时taskGroupId加载对应运行器
	 *
	 * @param pluginType
	 * @param pluginName
	 * @return
	 */
	public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName, TaskGroupContext taskGroupContext) {
		AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType, pluginName, taskGroupContext);

		switch (pluginType) {
		case READER:
			return new ReaderRunner(taskPlugin);
		case WRITER:
			return new WriterRunner(taskPlugin);
		default:
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
					String.format("插件[%s]的类型必须是[reader]或[writer]!", pluginName));
		}
	}


	public static synchronized JarLoader getJarLoader(PluginType pluginType, String pluginName) {
		return RWPluginCenter.getJarLoader(pluginType, pluginName);
	}
}
