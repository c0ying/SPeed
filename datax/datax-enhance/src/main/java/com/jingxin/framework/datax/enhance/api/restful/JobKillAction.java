package com.jingxin.framework.datax.enhance.api.restful;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.core.job.JobControllerPanel;
import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.VarResponseResult;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;

import io.netty.handler.codec.http.FullHttpRequest;

public class JobKillAction implements Action<VarResponseResult>{

	@Override
	public VarResponseResult act(RequestParam requestParam, FullHttpRequest req) {
		VarResponseResult result = new VarResponseResult();
		String jobIdStr = requestParam.queryParam("jobId");
		if (StringUtils.isBlank(jobIdStr)) {
			result.setStatus(false);
			result.setMsg("jobId must not be null");
		}else {
			try {
				JobControllerPanel.killJob(Long.parseLong(jobIdStr));
			} catch (Exception e) {
				e.printStackTrace();
				result.setStatus(false);
				result.setMsg("kill job occur error");
			}
		}
		return result;
	}

}
