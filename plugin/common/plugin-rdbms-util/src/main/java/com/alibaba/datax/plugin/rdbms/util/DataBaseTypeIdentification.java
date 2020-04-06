package com.alibaba.datax.plugin.rdbms.util;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.druid.util.JdbcUtils;

public class DataBaseTypeIdentification {

	public static DataBaseType getJobDataBaseType(Configuration configuration) {
		 List<Object> conns = configuration.getList(Constant.CONN_MARK, Object.class);
		 Configuration connConf = Configuration.from(conns.get(0).toString());
	     String jdbcUrl = connConf.getList(Key.JDBC_URL, String.class).get(0);
	     return getJdbcDataBaseType(jdbcUrl);
	}
	
	public static DataBaseType getWriterJobDataBaseType(Configuration configuration) {
		List<Object> conns = configuration.getList(Constant.CONN_MARK, Object.class);
		Configuration connConf = Configuration.from(conns.get(0).toString());
		String jdbcUrl = connConf.getString(Key.JDBC_URL);
		return getJdbcDataBaseType(jdbcUrl);
	}
	
	public static DataBaseType getTaskDateBaseType(Configuration configuration) {
		String jdbcUrl = configuration.getString(Key.JDBC_URL);
		return getJdbcDataBaseType(jdbcUrl);
	}
	
	public static DataBaseType getJdbcDataBaseType(String jdbcUrl) {
		String dbType = JdbcUtils.getDbType(jdbcUrl, "");
		return DataBaseType.dbNameOf(dbType);
	}
}
