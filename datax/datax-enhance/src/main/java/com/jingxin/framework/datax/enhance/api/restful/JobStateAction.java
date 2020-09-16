package com.jingxin.framework.datax.enhance.api.restful;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.jingxin.framework.datax.enhance.core.statistics.communication.CommunicationManagerDelegateFactory;
import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.VarResponseResult;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;

public class JobStateAction implements Action<VarResponseResult>{

	@Override
	public VarResponseResult act(RequestParam requestParam, FullHttpRequest req) {
		VarResponseResult result = new VarResponseResult();
		String jobIdStr = requestParam.queryParam("jobId");
		if (StringUtils.isBlank(jobIdStr)) {
			result.setStatus(false);
			result.setMsg("jobId must not be null");
		}else {
			try {
				Long jobId = Long.parseLong(jobIdStr);
				Communication jobCommunication = CommunicationManagerDelegateFactory.getJobCommunicationManager().getJobCommunication(jobId);
				result.put("jobId", jobId);
				result.put("state", jobCommunication);
			} catch (NumberFormatException e) {
				result.setStatus(false);
				result.setMsg("jobId is illegal");
			}
		}
		return result;
	}

}
