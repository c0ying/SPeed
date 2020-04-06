package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.bean.DataSourceConfig;

public class PoolJdbcConnectionFactory implements ConnectionFactory{
	
	private static ConcurrentHashMap<String, PoolJdbcConnectionFactory> factorys = new ConcurrentHashMap<>();
	private DataSource dataSource;
	private DataSourceConfig config;

	private PoolJdbcConnectionFactory(String jdbcUrl, String userName, String password) throws SQLException{
		this.config = new DataSourceConfig(jdbcUrl, userName, password);
		dataSource = DataSourcePools.initPool(config);
	};
	
	@Override
	public Connection getConnecttion() {
		try {
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                   return dataSource.getConnection();
                }
            }, 9, 1000L, false);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", config.getJdbcUrl()), e);
        }
	}

	@Override
	public Connection getConnecttionWithoutRetry() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			 throw DataXException.asDataXException(
	                    DBUtilErrorCode.CONN_DB_ERROR,
	                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", config.getJdbcUrl()), e);
		}
	}

	@Override
	public String getConnectionInfo() {
		return config.toString();
	}
	
	public void close() {
		DataSourcePools.close(String.valueOf(config.hashCode()));
	}
	
	public static synchronized PoolJdbcConnectionFactory getInstance(String jdbcUrl, String userName, String password) throws SQLException {
		DataSourceConfig config = new DataSourceConfig(jdbcUrl, userName, password);
		if (factorys.containsKey(String.valueOf(config.hashCode()))) {
			return factorys.get(String.valueOf(config.hashCode()));
		}else {
			PoolJdbcConnectionFactory poolFactory = new PoolJdbcConnectionFactory(jdbcUrl, userName, password);
			factorys.put(poolFactory.getConnectionInfo(), poolFactory);
			return poolFactory;
		}
	}

}
