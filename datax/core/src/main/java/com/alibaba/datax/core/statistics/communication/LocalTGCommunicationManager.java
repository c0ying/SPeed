package com.alibaba.datax.core.statistics.communication;

import org.apache.commons.lang3.Validate;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 只存储TG内各个Task的Communication
 * @author Cyh
 *
 */
public final class LocalTGCommunicationManager implements TGCommunicationManager{

	//jobId-tgId-taskId
    private static Map<Long, Map<Integer, Map<Integer, Communication>>> taskCommunicationMap =
    		new ConcurrentHashMap<Long, Map<Integer, Map<Integer, Communication>>>();
    private static LocalTGCommunicationManager localTGCommunicationManager = new LocalTGCommunicationManager();
    
    private LocalTGCommunicationManager() {}
    
    public static LocalTGCommunicationManager getInstance() {
    	return localTGCommunicationManager;
    }

	@Override
	public void register(long jobId, int taskGroupId, int task, Communication communication) {
		if (!taskCommunicationMap.containsKey(jobId)) {
			taskCommunicationMap.put(jobId, new ConcurrentHashMap<>());
		}
		if (!taskCommunicationMap.get(jobId).containsKey(taskGroupId)) {
			taskCommunicationMap.get(jobId).put(taskGroupId, new ConcurrentHashMap<>());
		}
		
		Map<Integer, Communication> taskCommunications = taskCommunicationMap.get(jobId).get(taskGroupId);
		if (!taskCommunications.containsKey(task)) {
			taskCommunications.put(task, new Communication());
		}
	}

	@Override
	public Map<Integer, Communication> getTaskCommunicationMap(long jobId, int taskGroupId) {
		return taskCommunicationMap.get(jobId).get(taskGroupId);
	}

	@Override
	public Communication getTG(long jobId, int taskGroupId) {
		return LocalJobCommunicationManager.getInstance().getTaskGroupCommunication(jobId, taskGroupId);
	}

	@Override
	public void updateTaskCommunication(long jobId, int taskGroupId, int task, Communication communication) {
		//do nothing
	}

	@Override
	public void clearAll(long jobId, int taskGroupId) {
		taskCommunicationMap.getOrDefault(jobId, Collections.emptyMap()).remove(taskGroupId);
	}

	@Override
	public void clearOne(long jobId, int taskGroupId, int task) {
		taskCommunicationMap.getOrDefault(jobId, Collections.emptyMap()).getOrDefault(taskGroupId, Collections.emptyMap()).remove(task);
	}

	public void clean(long jobId){
        taskCommunicationMap.remove(jobId);
    }

	public void updateTaskGroupCommunication(long jobId,final int taskGroupId,
											 final Communication communication) {
		Map<Integer, Communication> taskGroupCommunicationMap = LocalJobCommunicationManager.getInstance().getTaskGroupCommunicationMap(jobId);
		Validate.isTrue(taskGroupCommunicationMap.containsKey(
				taskGroupId), String.format("taskGroupCommunicationMap中没有注册JobId[%d]的Communication，" +
				"无法更新该taskGroup的信息", taskGroupId));
		if (taskGroupCommunicationMap.containsKey(taskGroupId)) {
			Communication oldCommunication = taskGroupCommunicationMap.get(taskGroupId);
			if (communication.getStartTimestamp() <= 0 && oldCommunication.getStartTimestamp() > 0) {
				communication.setStartTimestamp(oldCommunication.getStartTimestamp());
			}
			if (communication.getEndTimestamp() <= 0 && oldCommunication.getEndTimestamp() > 0) {
				communication.setEndTimestamp(oldCommunication.getEndTimestamp());
			}
			taskGroupCommunicationMap.put(taskGroupId, communication);
		}
	}
}