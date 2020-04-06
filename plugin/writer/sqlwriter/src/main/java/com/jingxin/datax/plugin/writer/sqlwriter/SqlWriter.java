package com.jingxin.datax.plugin.writer.sqlwriter;

import java.util.List;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.DataBaseTypeIdentification;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.EnhaceRdmsWriterTask;
import com.alibaba.datax.plugin.rdbms.writer.Key;


public class SqlWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck(){
//            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job();
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
//        private CommonRdbmsWriter.Task commonRdbmsWriterTask;
        private EnhaceRdmsWriterTask commonRdbmsWriterTask;
        private String writeMode;
        private DataBaseType dataBaseType;

        public Task(TaskGroupContext taskGroupContext) {
        	super(taskGroupContext);
		}
        
        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.dataBaseType = DataBaseTypeIdentification.getTaskDateBaseType(writerSliceConfig);
            this.commonRdbmsWriterTask = new EnhaceRdmsWriterTask();
            this.commonRdbmsWriterTask.setTaskContext(taskGroupContext);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
            this.writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "insert");
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
			if (dataBaseType== DataBaseType.Oracle && "replace".equals(writeMode)) {
				this.commonRdbmsWriterTask.startReplace(recordReceiver, 
						writerSliceConfig, super.getTaskPluginCollector());
			}else {
				this.commonRdbmsWriterTask.startWrite(recordReceiver,
						this.writerSliceConfig, super.getTaskPluginCollector());
			}
        }

        @Override
        public void post() {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver(){
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }

    }


}
