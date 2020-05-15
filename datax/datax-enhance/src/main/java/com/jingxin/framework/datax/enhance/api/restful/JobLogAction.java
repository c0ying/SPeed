package com.jingxin.framework.datax.enhance.api.restful;

import com.jingxin.framework.datax.enhance.core.job.JobRunningLogService;
import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.ResponseResult.ResponseStatus;
import com.jingxin.framework.datax.enhance.network.http.bean.response.VarResponseResult;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;

public class JobLogAction implements Action<VarResponseResult> {

    @Override
    public VarResponseResult act(RequestParam requestParam, FullHttpRequest req) {
        VarResponseResult responseResult = new VarResponseResult();
        String jobId = requestParam.queryParam("jobId");
        if (StringUtils.isBlank(jobId)){
            responseResult.setCode(ResponseStatus.REQUEST_FAIL.getCode());
            return responseResult;
        }else{
            responseResult.put("jobId", requestParam.queryParam("jobId"));
            responseResult.put("log",jobRunningLogService.getRunningLog(requestParam.queryParam("jobId")));
            return responseResult;
        }
    }

    private JobRunningLogService jobRunningLogService = new JobRunningLogService();
}
