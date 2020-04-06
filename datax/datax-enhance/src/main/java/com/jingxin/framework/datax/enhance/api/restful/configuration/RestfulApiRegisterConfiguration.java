package com.jingxin.framework.datax.enhance.api.restful.configuration;

import com.jingxin.framework.datax.enhance.api.restful.JobKillAction;
import com.jingxin.framework.datax.enhance.api.restful.JobStateAction;
import com.jingxin.framework.datax.enhance.api.restful.JobSubmitAction;
import com.jingxin.framework.datax.enhance.network.http.HandlerRegister;

public class RestfulApiRegisterConfiguration {

	static {
		HandlerRegister.POST("/jobAssign", new JobSubmitAction());
		HandlerRegister.GET("/jobKill", new JobKillAction());
		HandlerRegister.GET("/getJobState", new JobStateAction());
	}
}
