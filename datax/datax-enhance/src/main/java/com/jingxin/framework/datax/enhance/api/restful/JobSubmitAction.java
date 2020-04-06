package com.jingxin.framework.datax.enhance.api.restful;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.alibaba.datax.core.util.container.CoreConstant;
import com.jingxin.framework.datax.enhance.core.job.assign.JobAssignService;
import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.ResponseResult.ResponseStatus;
import com.jingxin.framework.datax.enhance.network.http.bean.response.VarResponseResult;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;
import com.jingxin.framework.datax.enhance.network.http.utils.Convert;

import io.netty.handler.codec.http.FullHttpRequest;

public class JobSubmitAction implements Action<VarResponseResult>{

	private JobAssignService service = new  JobAssignService();
	
	@Override
	public VarResponseResult act(RequestParam requestParam, FullHttpRequest req) {
		long jobId = System.currentTimeMillis();
		VarResponseResult result = new VarResponseResult();
		FileWriter fileWriter = null;
		String content = Convert.buf2Str(req.content());
	    try {
	    	File tmpDirctory = new File(CoreConstant.DATAX_HOME, "tmp");
	    	if (!tmpDirctory.exists()) tmpDirctory.mkdir();
			
	    	File tmpFile = File.createTempFile("Datax-"+String.valueOf(jobId), ".json", tmpDirctory);
			fileWriter = new FileWriter(tmpFile);
			fileWriter.write(content);
			fileWriter.flush();
			
			service.assign(jobId, tmpFile.getAbsolutePath());
			
			result.put("jobId", jobId);
		} catch (Throwable e) {
			e.printStackTrace();
			result.setCode(ResponseStatus.FAIL.getCode());
			result.setMsg(ResponseStatus.FAIL.getText());
		} finally {
			if (fileWriter != null) {
				 try {
					fileWriter.close();
				} catch (IOException e) {}
			}
		}
	    return result;
	}

}
