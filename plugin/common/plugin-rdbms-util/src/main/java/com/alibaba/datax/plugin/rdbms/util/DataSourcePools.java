package com.alibaba.datax.plugin.rdbms.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.rdbms.bean.DataSourceConfig;
import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.filter.logging.Slf4jLogFilter;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;

/**
 * 全新的数据连接池创建工具
 * @author Cyh
 *
 */
public abstract class DataSourcePools {
	
	private static Logger logger = LoggerFactory.getLogger(DataSourcePools.class);

	private static ConcurrentHashMap<String, DataSourceConfig> meta = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, DataSource> pools = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, Lock> locks = new ConcurrentHashMap<>();
	
	public static final DataSource getPool(DataSourceConfig config) throws SQLException {
		String id = config.getJdbcUrl();
		String code = String.valueOf(config.hashCode());
		if(pools.containsKey(code)) {
			logger.debug("return existed dataSource id:{}", id);
			return pools.get(code);
		}else {
			Lock initLock = null;
			//确保生成唯一的Lock锁
			if (!locks.containsKey(code)) {
				logger.trace("get init dataSource lock id:{}", id);
				locks.put(code, new ReentrantLock());
			}
			initLock = locks.get(code);
			try {
				initLock.lockInterruptibly();
				//再次判断是否已经初始化。
				//在并发运行时，后入线程可能会再次初始化，再次判断是否已经存在，再初始化。
				if(pools.containsKey(code)) {
					logger.debug("init dataSource concurrenty,found dataSource had beed inited,skipped init id:{}", id);
					return pools.get(code);
				}else {
					logger.debug("init dataSource id:{}", id);
					config.setDataBaseType(DataBaseTypeIdentification.getJdbcDataBaseType(config.getJdbcUrl()));
					DataSource pool = initPool(config);
					pools.put(code, pool);
					meta.put(id, config);
					return pool;
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}finally {
				initLock.unlock();
				locks.remove(code);
			}
		}
	}
	
	public static final void close(String code) {
		logger.info("closing code:{} dataSource", code);
		DataSourceConfig dataSourceConfig = meta.get(code);
		if (dataSourceConfig != null) {
			if(pools.containsKey(code)) {
				((DruidDataSource)pools.get(code)).close();
				pools.remove(code);
			}
		}
	}
	
	public static final void closeAll() {
		logger.info("closing all dataSource");
		for (Entry<String, DataSource> pool : pools.entrySet()) {
			close(pool.getKey());
		}
	}
	
	public static final DataSource initPool(DataSourceConfig config) throws SQLException { 
		DruidDataSource dataSource =  new DruidDataSource();  
		dataSource.setInitialSize(5);
		dataSource.setMinIdle(5);
		dataSource.setMaxActive(30);
		dataSource.setMaxWait(30000);
		dataSource.setTestWhileIdle(true);
		dataSource.setTestOnBorrow(true);
		dataSource.setTestOnReturn(false);
		dataSource.setMinEvictableIdleTimeMillis(600000);
		dataSource.setMaxEvictableIdleTimeMillis(28800000);
		dataSource.setTimeBetweenEvictionRunsMillis(350000);
		dataSource.setDefaultAutoCommit(false);
		dataSource.setRemoveAbandoned(true);
		dataSource.setKeepAlive(true);
		try {
			dataSource.setFilters("stat");
			List<Filter> proxyFilters = new ArrayList<>();
			proxyFilters.add(druidLogFilter());
			dataSource.setProxyFilters(proxyFilters);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		  
	   String jdbcUrl = config.getJdbcUrl();
	   String username = config.getUserName();
	   String password = config.getPassWord();
	   String dbType = JdbcUtils.getDbType(jdbcUrl, "");
		if (dbType.equals(JdbcConstants.ORACLE)) {
			// for oracle(新版mysql可能可以使用，暫未確定)  
//			dataSource.setPoolPreparedStatements(true);
//			dataSource.setMaxPoolPreparedStatementPerConnectionSize(50);
			dataSource.setValidationQuery("SELECT 1 FROM DUAL");
			dataSource.setUsername(username);
			dataSource.setPassword(password);
			dataSource.setUrl(jdbcUrl);
		} else {
			dataSource.setValidationQuery("SELECT 1");
			dataSource.setUsername(username);
			dataSource.setPassword(password);
			dataSource.setUrl(jdbcUrl);
		} 
		dataSource.init();
		return dataSource;    
	}

	protected static Slf4jLogFilter druidLogFilter(){
		Slf4jLogFilter slf4jLogFilter = new Slf4jLogFilter();
		slf4jLogFilter.setStatementExecutableSqlLogEnable(false);
		slf4jLogFilter.setDataSourceLogEnabled(false);
		slf4jLogFilter.setConnectionLogEnabled(false);
		slf4jLogFilter.setConnectionLogErrorEnabled(true);
		slf4jLogFilter.setStatementLogEnabled(true);
		slf4jLogFilter.setStatementExecuteAfterLogEnabled(true);
		slf4jLogFilter.setStatementPrepareAfterLogEnabled(false);
		slf4jLogFilter.setStatementPrepareCallAfterLogEnabled(false);
		slf4jLogFilter.setStatementExecuteQueryAfterLogEnabled(true);
		slf4jLogFilter.setStatementExecuteUpdateAfterLogEnabled(true);
		slf4jLogFilter.setStatementExecuteBatchAfterLogEnabled(true);
		slf4jLogFilter.setStatementCloseAfterLogEnabled(false);
		slf4jLogFilter.setStatementParameterSetLogEnabled(true);
		slf4jLogFilter.setStatementLogErrorEnabled(true);
		return slf4jLogFilter;
	}
}
