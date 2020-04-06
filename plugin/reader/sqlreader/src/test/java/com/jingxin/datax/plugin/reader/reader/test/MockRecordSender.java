package com.jingxin.datax.plugin.reader.reader.test;

import com.alibaba.datax.common.element.DefaultRecord;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;

public class MockRecordSender implements RecordSender{

	@Override
	public Record createRecord() {
		return new DefaultRecord();
	}

	@Override
	public void sendToWriter(Record record) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void terminate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

}
