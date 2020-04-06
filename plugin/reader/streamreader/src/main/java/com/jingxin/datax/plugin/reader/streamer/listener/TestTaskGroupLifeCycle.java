package com.jingxin.datax.plugin.reader.streamer.listener;

import com.alibaba.datax.common.base.TaskGroupContext;
import com.alibaba.datax.common.spi.TaskGroupLifeCycle;

public class TestTaskGroupLifeCycle implements TaskGroupLifeCycle{

	@Override
	public void init(TaskGroupContext taskGroupContext) {
		System.out.println("TaskGroup inited");
	}

	@Override
	public void destory(TaskGroupContext taskGroupContext) {
		System.out.println("TaskGroup destoryed");
	}

	@Override
	public void beforeStart(TaskGroupContext context) {
		// TODO Auto-generated method stub
		
	}

}
