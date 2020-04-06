package com.alibaba.datax.plugin.rdbms.reader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.PreCheckTask;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.DataBaseTypeIdentification;
import com.alibaba.datax.plugin.rdbms.util.PoolJdbcConnectionFactory;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.util.listener.DataSourceInitManager;
import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;

public class CommonRdbmsReader {

	public static class Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);
		private DataBaseType dataBaseType;

		public Job() {}

		public void init(Configuration originalConfig) {
			dataBaseType = DataBaseTypeIdentification.getJobDataBaseType(originalConfig);

			OriginalConfPretreatmentUtil.doPretreatment(originalConfig, dataBaseType);

			LOG.debug("After job init(), job config now is:[\n{}\n]", originalConfig.toJSON());
		}

		public void preCheck(Configuration originalConfig) {
			/* 检查每个表是否有读权限，以及querySql跟splik Key是否正确 */
			Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
			String splitPK = queryConf.getString(Key.SPLIT_PK);
			List<Object> connList = queryConf.getList(Constant.CONN_MARK, Object.class);
			String username = queryConf.getString(Key.USERNAME);
			String password = queryConf.getString(Key.PASSWORD);
			ExecutorService exec;
			if (connList.size() < 10) {
				exec = Executors.newFixedThreadPool(connList.size());
			} else {
				exec = Executors.newFixedThreadPool(10);
			}
			Collection<PreCheckTask> taskList = new ArrayList<PreCheckTask>();
			for (int i = 0, len = connList.size(); i < len; i++) {
				Configuration connConf = Configuration.from(connList.get(i).toString());
				PreCheckTask t = new PreCheckTask(username, password, connConf, dataBaseType, splitPK);
				taskList.add(t);
			}
			List<Future<Boolean>> results = Lists.newArrayList();
			try {
				results = exec.invokeAll(taskList);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			for (Future<Boolean> result : results) {
				try {
					result.get();
				} catch (ExecutionException e) {
					DataXException de = (DataXException) e.getCause();
					throw de;
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			exec.shutdownNow();
		}

		public List<Configuration> split(Configuration originalConfig, int adviceNumber) {
			return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
		}

		public void post(Configuration originalConfig) {
			// do nothing
		}

		public void destroy(Configuration originalConfig) {
			// do nothing
		}

	}

	public static class Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		private int taskGroupId = -1;
		private int taskId = -1;

		private DataBaseType dataBaseType;
		private String username;
		private String password;
		private String jdbcUrl;
		private String mandatoryEncoding;

		// 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
		private String basicMsg;
		
		private boolean multiReadModel = false;
		private PoolJdbcConnectionFactory connectionFactory;

		private TaskGroupContext taskContext;

		public Task(int taskGropuId, int taskId) {
			this.taskGroupId = taskGropuId;
			this.taskId = taskId;
		}

		public void init(Configuration readerSliceConfig) {
			/* for database connection */
			this.username = readerSliceConfig.getString(Key.USERNAME);
			this.password = readerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
			this.dataBaseType = DataBaseTypeIdentification.getJdbcDataBaseType(jdbcUrl);

			Integer readerChannel = readerSliceConfig.getInt(Key.READER_CHANNEL);
			if (readerChannel != null && readerChannel > 1) {
				multiReadModel = true;
				try {
					this.connectionFactory = PoolJdbcConnectionFactory.getInstance(jdbcUrl, username, password);
				} catch (SQLException e) {
					throw RdbmsException.asConnException(dataBaseType, e, username, jdbcUrl);
				}
			}
			// ob10的处理
			if (this.jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)
					&& this.dataBaseType == DataBaseType.MySql) {
				String[] ss = this.jdbcUrl
						.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
				if (ss.length != 3) {
					throw DataXException.asDataXException(DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR,
							"JDBC OB10格式错误，请联系askdatax");
				}
				LOG.info("this is ob1_0 jdbc url.");
				this.username = ss[1].trim() + ":" + this.username;
				this.jdbcUrl = ss[2];
				LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
			}

			this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

			basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

		}
		
		public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
				TaskPluginCollector taskPluginCollector, int fetchSize) {
			boolean isTableMode = readerSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
			Integer readerChannel = readerSliceConfig.getInt(Key.READER_CHANNEL);
			List<Configuration> tasks = new ArrayList<>();
			if (multiReadModel && isTableMode) {
				tasks.addAll(ReaderSplitUtil.doSplit(readerSliceConfig, readerChannel));
			}else {
				tasks.add(readerSliceConfig);
			}
			List<Thread> readSubTasks = new ArrayList<>();
			for (Configuration task : tasks) {
				Thread readSubTask = new Thread(() -> {
					String querySql = task.getString(Key.QUERY_SQL);
					String table = task.getString(Key.TABLE);
					PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

					Connection conn = null;
					ResultSet rs = null;
					try {
						LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, basicMsg);
						PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
						queryPerfRecord.start();
						if (multiReadModel) {
							conn = connectionFactory.getConnecttion();
						}else {
							conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);
						}
						// session config .etc related
						DBUtil.dealWithSessionConfig(conn, task, this.dataBaseType, basicMsg);
						
						rs = DBUtil.query(conn, querySql, fetchSize);
						queryPerfRecord.end();

						// 这个统计干净的result_Next时间
						PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
						allResultPerfRecord.start();

						long rsNextUsedTime = 0;
						long lastTime = System.nanoTime();
						while (rs.next()) {
							rsNextUsedTime += (System.nanoTime() - lastTime);
							ResultSetReadProxy.transportOneRecord(recordSender, rs, mandatoryEncoding, taskPluginCollector);
							lastTime = System.nanoTime();
						}

						allResultPerfRecord.end(rsNextUsedTime);
						// 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
						LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);

					} catch (Exception e) {
						throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
					} finally {
						DBUtil.closeDBResources(null, conn);
					}
				});
				readSubTasks.add(readSubTask);
				if (readSubTasks.size() > 1) {
					for (Thread subTask : readSubTasks) {
						subTask.start();
					}
					try {
						Thread.sleep(50000);
					} catch (InterruptedException e) {
						return;
					}
					for (Thread thread : readSubTasks) {
						try {
							thread.join();
						} catch (InterruptedException e) {}
					}
				}else {
					readSubTask.run();
				}
			}
		}

		public void post(Configuration originalConfig) {
			// do nothing
		}

		public void destroy(Configuration originalConfig) {
			if (this.connectionFactory != null) {
				this.connectionFactory.close();
			}
		}

		public void setTaskContext(TaskGroupContext taskContext) {
			this.taskContext = taskContext;
		}
	}
}
