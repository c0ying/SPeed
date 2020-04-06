package com.jingxin.framework.datax.enhance.network.http;

import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.route.Router;

public class HandlerRegister {

	private static Router router = new Router();
	// private GetVerifyCodeAct getVerifyCodeAct;//w

	public static Router getRouter() {
		return router;
	}
	
	public static void GET(String url, Action<?> action) {
		router.GET(url, action);
	}
	public static void POST(String url, Action<?> action) {
		router.POST(url, action);
	}
}
