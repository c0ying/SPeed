package com.jingxin.datax.plugin.writer.sql.test;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.jingxin.datax.plugin.writer.sqlwriter.SqlWriter;

public class SqlWriterTest {

private Configuration configuration = Configuration.newDefault();
	
	@Before
	public void init() {
//		configuration.set("name", "sqlreader");
//		Configuration subConfig = Configuration.newDefault();
		configuration.set("username", "root");
		configuration.set("password", "mysql123");
//		configuration.set("column", Arrays.asList("*"));
		Configuration connectConfig = Configuration.newDefault();
		connectConfig.set("jdbcUrl", "jdbc:mysql://localhost:3306/gsgzsq");
		connectConfig.set("table", Arrays.asList("meta_clog"));
		configuration.set("connection", Arrays.asList(connectConfig));
//		configuration.set("parameter", configuration);
	}
	
	@Test
	public void job() {
		sqlWriterJob.setPluginJobConf(configuration);
		sqlWriterJob.init();
		sqlWriterJob.preCheck();
		sqlWriterJob.split(5);
		sqlWriterJob.post();
		sqlWriterJob.destroy();
	}
	
	private SqlWriter.Job sqlWriterJob = new SqlWriter.Job();
}
