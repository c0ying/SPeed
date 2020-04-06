package com.alibaba.datax.core.transport.exchanger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.transformer.TransformerExecution;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

public class BufferedRecordTransformerSender extends TransformerExchanger implements RecordSender{

    private final Channel channel;

    private final Configuration configuration;

    private final List<Record> buffer;

    private int bufferSize;

    protected final int byteCapacity;

    private final AtomicInteger memoryBytes = new AtomicInteger(0);

    private int bufferIndex = 0;

    private Class<? extends Record> RECORD_CLASS;

    private volatile boolean shutdown = false;
    private volatile boolean terminate = false;
    
    private LinkedBlockingQueue<Record> transformerQueue = new LinkedBlockingQueue<>(3000);

    private Thread asyncTransformerThread;

    @SuppressWarnings("unchecked")
    public BufferedRecordTransformerSender(final int taskGroupId, final int taskId,
                                              final Channel channel, final Communication communication,
                                              final TaskPluginCollector pluginCollector,
                                              final List<TransformerExecution> tInfoExecs) {
        super(taskGroupId, taskId, communication, tInfoExecs, pluginCollector);
        assert null != channel;
        assert null != channel.getConfiguration();

        this.channel = channel;
        this.configuration = channel.getConfiguration();

        this.bufferSize = configuration
                .getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);
        this.buffer = new ArrayList<Record>(bufferSize);

        //channel的queue默认大小为8M，原来为64M
        this.byteCapacity = configuration.getInt(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);

        try {
            RECORD_CLASS = ((Class<? extends Record>) Class
                    .forName(configuration.getString(
                            CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.alibaba.datax.core.transport.record.DefaultRecord")));
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
        asyncTransformerThread = new Thread(new TransformerThread());
        asyncTransformerThread.setName("TG-"+taskGroupId+"-taskId-"+taskId+"-asyncTransformerThread");
        asyncTransformerThread.start();
    }

    @Override
    public Record createRecord() {
        try {
            return RECORD_CLASS.newInstance();
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record) {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }

        Validate.notNull(record, "record不能为空.");

//        record = doTransformer(record);

//        if(record == null){
//            return;
//        }
        
        try {
			this.transformerQueue.put(record);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

//        if (record.getMemorySize() > this.byteCapacity) {
//            this.pluginCollector.collectDirtyRecord(record, new Exception(String.format("单条记录超过大小限制，当前限制为:%s", this.byteCapacity)));
//            return;
//        }
//
//        boolean isFull = (this.bufferIndex >= this.bufferSize || this.memoryBytes.get() + record.getMemorySize() > this.byteCapacity);
//        if (isFull) {
//            flush();
//        }
//
//        this.buffer.add(record);
//        this.bufferIndex++;
//        memoryBytes.addAndGet(record.getMemorySize());
    }

    @Override
    public void flush() {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        this.channel.pushAll(this.buffer);
        //和channel的统计保持同步
        doStat();
        this.buffer.clear();
        this.bufferIndex = 0;
        this.memoryBytes.set(0);
    }

    @Override
    public void terminate() {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        terminate=true;
        try {
			asyncTransformerThread.join();
		} catch (InterruptedException e) {}
        flush();
        this.channel.pushTerminate(TerminateRecord.get());
    }

    @Override
    public void shutdown() {
        shutdown = true;
        try {
            buffer.clear();
            channel.clear();
            transformerQueue.clear();
            asyncTransformerThread.interrupt();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public class TransformerThread implements Runnable{

		@Override
		public void run() {
			while(true) {
				try {
					if (terminate && transformerQueue.isEmpty()) {
			        	flush();
			        	channel.pushTerminate(TerminateRecord.get());
			        	break;
					}
					Record record = transformerQueue.take();
					
					record = doTransformer(record);
					
					if(record == null){
			            return;
			        }
					
					if (record.getMemorySize() > byteCapacity) {
			            pluginCollector.collectDirtyRecord(record, new Exception(String.format("单条记录超过大小限制，当前限制为:%s", byteCapacity)));
			            return;
			        }
					
			        boolean isFull = (bufferIndex >= bufferSize || memoryBytes.get() + record.getMemorySize() > byteCapacity);
			        if (isFull) {
			            flush();
			        }
			        buffer.add(record);
			        bufferIndex++;
			        memoryBytes.addAndGet(record.getMemorySize());
				} catch (InterruptedException e) {
					break;
				} 
			}
		}
    	
    }
}
