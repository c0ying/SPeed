package com.jingxin.framework.datax.enhance.api.restful;

import com.jingxin.framework.datax.enhance.core.job.JobControllerPanel;
import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.VarResponseResult;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;

public class JobKillAction implements Action<VarResponseResult>{

	private JobControllerPanel jobControllerPanel;

	public JobKillAction(){
		jobControllerPanel = JobControllerPanel.instance;
	}

	@Override
	public VarResponseResult act(RequestParam requestParam, FullHttpRequest req) {
		VarResponseResult result = new VarResponseResult();
		String jobIdStr = requestParam.queryParam("jobId");
		if (StringUtils.isBlank(jobIdStr)) {
			result.setStatus(false);
			result.setMsg("jobId must not be null");
		}else {
			try {
				jobControllerPanel.killJob(Long.parseLong(jobIdStr));
			} catch (Exception e) {
				e.printStackTrace();
				result.setStatus(false);
				result.setMsg("kill job occur error");
			}
		}
		return result;
	}

}
