package com.jingxin.framework.datax.enhance.core;

import com.alibaba.datax.core.util.ConfigurationInitialize;
import com.jingxin.framework.datax.enhance.api.restful.configuration.RestfulApiRegisterConfiguration;
import com.jingxin.framework.datax.enhance.core.common.meta.MetaResource;
import com.jingxin.framework.datax.enhance.network.http.HttpServer;

/**
 * SPeed DataX Web 启动入口
 * @author cyh
 *
 */
public class HttpEntry {
	
	private static HttpServer httpServer;

	public static void start() throws Exception {
		//DataX核心配置初始化
		ConfigurationInitialize.init();

		MetaResource.init(ConfigurationInitialize.CORE_CONFIGURATION);
		Integer webPort = ConfigurationInitialize.CORE_CONFIGURATION.getInt("webEntry.port");
		if (webPort == null) webPort = 8081;
		httpServer = new HttpServer();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run()
			{
				System.out.println("SPeed DataX Http Server stopping.....");
				httpServer.stop();
				MetaResource.destory();
			}
		}));
		//初始化SPeed DataX接口注册
		new RestfulApiRegisterConfiguration();
        System.out.println("SPeed DataX Http Server listening on "+webPort+" ...");
        httpServer.start(webPort);
        
	}
	
	public static void stop() {
		System.exit(1);
	}
	
	public static void main(String[] args) throws Exception {
		start();
	}
}
