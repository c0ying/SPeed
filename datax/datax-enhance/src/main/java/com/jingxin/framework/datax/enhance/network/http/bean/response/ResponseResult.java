package com.jingxin.framework.datax.enhance.network.http.bean.response;

import java.io.Serializable;

/**
 * 包含请求状态信息的数据封装Bean
 * @author cyh
 *
 * @param <T>
 */
public class ResponseResult<T> implements Serializable{
	private static final long serialVersionUID = 8336737396447713426L;
	private boolean status;
	private String msg;
	private int code;
	private T data;
	
	public ResponseResult() {
		code = ResponseStatus.SUCCESS.getCode();
		msg = ResponseStatus.SUCCESS.getText();
		status = true;
	}
	
	public ResponseResult(int code, String msg) {
		this.code = code;
		this.msg = msg;
		if (code != ResponseStatus.SUCCESS.getCode()) {
			status = false;
		}else {
			status = true;
		}
	}
	
	public ResponseResult(T data) {
		this();
		this.data = data;
	}
	
	public T getData() {
		return data;
	}
	public void setData(T data) {
		this.data = data;
	}
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
		if (status) {
			this.code = ResponseStatus.SUCCESS.getCode();
			this.msg = ResponseStatus.SUCCESS.getText();
		}else {
			this.code = ResponseStatus.SUCCESS.getCode();
			this.msg = ResponseStatus.SUCCESS.getText();
		}
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	
	//返回状态
	public static enum ResponseStatus{
		SUCCESS(200,"成功"),
		REQUEST_FAIL(400,"请求有误"),
		SERVER_ERR(500,"服务端异常"),
		FAIL(501,"失败");
		
		private int code;
		private String text;
		
		private ResponseStatus(int code, String text) {
			this.code = code;
			this.text = text;
		}

		public int getCode() {
			return code;
		}

		public String getText() {
			return text;
		}

	}
}
