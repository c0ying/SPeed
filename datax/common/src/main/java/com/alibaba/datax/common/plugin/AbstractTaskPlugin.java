package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.base.TaskGroupContext;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractTaskPlugin extends AbstractPlugin {

    //TaskPlugin 应该具备taskId
    private int taskGroupId;
    private int taskId;
    private TaskPluginCollector taskPluginCollector;
    //@cyh;190116;TaskGroup公用的context
    protected TaskGroupContext taskGroupContext;
    
    public AbstractTaskPlugin() {}
    
    public TaskPluginCollector getTaskPluginCollector() {
        return taskPluginCollector;
    }

    public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId) {
        this.taskGroupId = taskGroupId;
    }
    public TaskGroupContext getTaskGroupContext() {
    	return taskGroupContext;
    }
}
