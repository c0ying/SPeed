package com.jingxin.framework.datax.enhance.network.http.bean.response;

import java.util.HashMap;
import java.util.Map;

/**
 * 使用Map存储数据，方便转换多种不定名称属性的Json
 * @author cyh
 *
 */
public class VarResponseResult extends ResponseResult<Map<String,Object>>{

	private static final long serialVersionUID = -478810578862616994L;
	
	private Map<String, Object> data = new HashMap<String, Object>(1);
	
	public VarResponseResult() {
		super();
		setData(data);
	}
	
	public void put(String key, Object value) {
		data.put(key, value);
	}
}
