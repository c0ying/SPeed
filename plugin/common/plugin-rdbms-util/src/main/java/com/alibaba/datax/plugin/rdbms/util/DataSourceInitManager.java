package com.alibaba.datax.plugin.rdbms.util;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.base.TaskGroupInfo;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.bean.DataSourceConfig;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataSourcePools;
import com.alibaba.druid.pool.DruidDataSource;

public class DataSourceInitManager {

	public static final String RDATASOURCE = "READ-DATASOURCE";
	public static final String WDATASOURCE = "WRITE-DATASOURCE";

	public static void init(TaskGroupContext taskGroupContext) {
		List<Configuration> taskConfigs = ((TaskGroupInfo)taskGroupContext.getTaskGroupInfo()).getTaskConfigs();
		if(!taskGroupContext.containsKey(RDATASOURCE)){
			String jdbcUrl = taskConfigs.get(0).getString("reader.parameter.".concat(Key.JDBC_URL));
			String userName = taskConfigs.get(0).getString("reader.parameter.".concat(Key.USERNAME));
			String passWord = taskConfigs.get(0).getString("reader.parameter.".concat(Key.PASSWORD));
			if (StringUtils.isNoneBlank(jdbcUrl) && StringUtils.isNoneBlank(userName) && StringUtils.isNoneBlank(passWord)) {
				DataSourceConfig dataSourceConfig = new DataSourceConfig();
				dataSourceConfig.setJdbcUrl(jdbcUrl);
				dataSourceConfig.setUserName(userName);
				dataSourceConfig.setPassWord(passWord);
				try {
					DataSource dataSource = DataSourcePools.initPool(dataSourceConfig);
					taskGroupContext.put(RDATASOURCE, dataSource);
				} catch (SQLException e) {
					e.printStackTrace();
					throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
				}
			}
		}
		if (!taskGroupContext.containsKey(WDATASOURCE)) {
			String jdbcUrl = taskConfigs.get(0).getString("writer.parameter.".concat(Key.JDBC_URL));
			String userName = taskConfigs.get(0).getString("writer.parameter.".concat(Key.USERNAME));
			String passWord = taskConfigs.get(0).getString("writer.parameter.".concat(Key.PASSWORD));
			if (StringUtils.isNoneBlank(jdbcUrl) && StringUtils.isNoneBlank(userName) && StringUtils.isNoneBlank(passWord)) {
				DataSourceConfig dataSourceConfig = new DataSourceConfig();
				dataSourceConfig.setJdbcUrl(jdbcUrl);
				dataSourceConfig.setUserName(userName);
				dataSourceConfig.setPassWord(passWord);
				try {
					DataSource dataSource = DataSourcePools.initPool(dataSourceConfig);
					taskGroupContext.put(WDATASOURCE, dataSource);
				} catch (SQLException e) {
					e.printStackTrace();
					throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, e);
				}
			}
		}
	}

	public static void destory(TaskGroupContext taskGroupContext) {
		if(taskGroupContext.containsKey(RDATASOURCE)){
			DruidDataSource druidDataSource = (DruidDataSource) taskGroupContext.get(RDATASOURCE);
			if (!druidDataSource.isClosed()){
				druidDataSource.close();
			}
		}
		if (taskGroupContext.containsKey(WDATASOURCE)) {
			DruidDataSource druidDataSource = (DruidDataSource) taskGroupContext.get(WDATASOURCE);
			if (!druidDataSource.isClosed()) {
				druidDataSource.close();
			}
		}
	}

}
