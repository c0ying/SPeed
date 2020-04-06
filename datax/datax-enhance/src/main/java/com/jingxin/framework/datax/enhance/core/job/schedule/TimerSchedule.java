package com.jingxin.framework.datax.enhance.core.job.schedule;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.core.util.ConfigParser;
import com.jingxin.framework.datax.enhance.core.job.assign.JobAssignService;
import com.jingxin.framework.datax.enhance.core.job.assign.JobAssignService.JobSubmiter;

public class TimerSchedule {
	private static final Logger LOG = LoggerFactory.getLogger(TimerSchedule.class);
	
	private JobAssignService jobAssignService;
	
	public TimerSchedule(JobAssignService jobAssignService) {
		this.jobAssignService = jobAssignService;
	}

	public void interval(String jobPath, int second) {
		Timer timer = new Timer(true);
		timer.scheduleAtFixedRate(new DefaultTimerJob(jobPath),TimeUnit.MINUTES.toMillis(1), TimeUnit.SECONDS.toMillis(second));
		LOG.info("register interval submit task");
	}
	
	class DefaultTimerJob extends TimerTask{

		private String jobPath;
		
		public DefaultTimerJob(String jobPath) {
			this.jobPath = jobPath;
		}
		
		@Override
		public void run() {
			JobSubmiter jobSubmiter = jobAssignService.new JobSubmiter(ConfigParser.parse(jobPath));
			jobSubmiter.setId(System.currentTimeMillis());
			long jobId = jobAssignService.assign(System.currentTimeMillis(), jobSubmiter);
			LOG.info("auto interval submit job;jobId:{},task:{}",jobId,jobSubmiter.getConfiguration().toJSON());
		}
		
	}
}
