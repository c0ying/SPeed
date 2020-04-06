package com.jingxin.framework.datax.enhance.core.job.assign;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.jingxin.framework.datax.enhance.core.job.schedule.TimerSchedule;


public class JobAssignService {
	
	private static final Logger LOG = LoggerFactory.getLogger(JobAssignService.class);

	private ExecutorService threadPool = Executors.newFixedThreadPool(20);
	private Engine engine = new Engine();
	private TimerSchedule timerSchedule = new TimerSchedule(this);
	
	public long assign(String jobPath) throws Throwable {
		long jobId = System.currentTimeMillis();
		assign(jobId, jobPath);
		return jobId;
	}
	
	public long assign(long jobId, String jobPath) throws Throwable {
		Configuration configuration = ConfigParser.parse(jobPath);
		configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
		ConfigurationValidate.doValidate(configuration);
		
//		String javaHome = configuration.getString("entry.java_home");
//		File libFile = new File(CoreConstant.DATAX_HOME, "lib");
//		String mainClass = "com.alibaba.datax.core.Engine";
//		Runtime.getRuntime().exec(
//				new String[] {javaHome+"java","-cp",libFile.getAbsolutePath(),mainClass,"-Dfile.encoding=UTF-8", "--Ddatax.home="+CoreConstant.DATAX_HOME});
		
		JobSubmiter job = new JobSubmiter(configuration);
		threadPool.submit(job);
		int interval = configuration.getInt("job.setting.interval", 0);
		if (interval > 0) {
			timerSchedule.interval(jobPath, interval);
		}
		return jobId;
	}
	
	public long assign(long jobId, JobSubmiter jobSubmiter) {
		jobSubmiter.setId(jobId);
		threadPool.submit(jobSubmiter);
		return jobId;
	}
	
	public void shutdown() {
		threadPool.shutdownNow();
	}
	
	public class JobSubmiter implements Runnable{
		
		private Configuration configuration;
		
		public JobSubmiter(Configuration configuration) {
			this.configuration = configuration;
		}
		
		public void setId(long jobId) {
			configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		@Override
		public void run() {
			try {
				startEngine();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		
		protected void startEngine() throws Throwable {
	        LOG.info("starting jobId:{}", this.configuration.get(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID));
	        engine.start(configuration);
		}
		
	}
}
