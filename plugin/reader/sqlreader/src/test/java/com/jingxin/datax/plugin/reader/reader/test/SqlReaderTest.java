package com.jingxin.datax.plugin.reader.reader.test;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.util.Configuration;
import com.jingxin.datax.plugin.reader.reader.SqlReader;

public class SqlReaderTest {

	
	public Configuration initJobConfig() {
		Configuration configuration = Configuration.newDefault();
//		configuration.set("name", "sqlreader");
//		Configuration subConfig = Configuration.newDefault();
		configuration.set("username", "root");
		configuration.set("password", "mysql123");
		configuration.set("column", Arrays.asList("*"));
		Configuration connectConfig = Configuration.newDefault();
		connectConfig.set("jdbcUrl", Arrays.asList("jdbc:mysql://localhost:3306/gsgzsq"));
		connectConfig.set("table", Arrays.asList("meta_clog"));
		configuration.set("connection", Arrays.asList(connectConfig));
		configuration.set("readChannel", 2);
//		configuration.set("parameter", configuration);
		return configuration;
	}
	
	public Configuration initTaskConfig() {
		Configuration configuration = Configuration.newDefault();
//		configuration.set("name", "sqlreader");
//		Configuration subConfig = Configuration.newDefault();
		configuration.set("username", "root");
		configuration.set("password", "mysql123");
		configuration.set("column", Arrays.asList("*"));
		configuration.set("jdbcUrl", "jdbc:mysql://192.168.50.186:13306/my");
		configuration.set("table", Arrays.asList("bt"));
		configuration.set("readerChannel", 2);
		configuration.set("querySql", "select * from bt");
//		configuration.set("parameter", configuration);
		return configuration;
	}
	
	@Test
	public void job() {
		Configuration configuration = initJobConfig();
		sqlReaderJob.setPluginJobConf(configuration);
		sqlReaderJob.init();
//		sqlReaderJob.preCheck();
		sqlReaderJob.split(5);
		sqlReaderJob.post();
		sqlReaderJob.destroy();
	}
	
	@Test
	public void task() {
		Configuration configuration = initTaskConfig();
		sqlReaderTask.setPluginJobConf(configuration);
		sqlReaderTask.init();
		sqlReaderTask.startRead(new MockRecordSender());
		sqlReaderTask.post();
		sqlReaderTask.destroy();
	}
	
	private SqlReader.Job sqlReaderJob = new SqlReader.Job();
	private SqlReader.Task sqlReaderTask = new SqlReader.Task(new TaskGroupContext());
}
