package com.alibaba.datax.core.statistics.communication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 只存储TG内各个Task的Communication
 * @author Cyh
 *
 */
public final class LocalTGCommunicationManager implements TGCommunicationManager{
//    private static Map<Long, Map<Integer, Communication>> taskGroupCommunicationMap =
//            new ConcurrentHashMap<Long, Map<Integer, Communication>>();
//    
	//jobId-tgId-taskId
    private static Map<Long, Map<Integer, Map<Integer, Communication>>> taskCommunicationMap =
    		new ConcurrentHashMap<Long, Map<Integer, Map<Integer, Communication>>>();
    private static LocalTGCommunicationManager localTGCommunicationManager = new LocalTGCommunicationManager();
    
    private LocalTGCommunicationManager() {}
    
    public static LocalTGCommunicationManager getInstance() {
    	return localTGCommunicationManager;
    }

//    public static void registerTaskGroupCommunication(long jobId,
//            int taskGroupId, Communication communication) {
//    	if (!taskGroupCommunicationMap.containsKey(jobId)) {
//    		Map<Integer, Communication> tmp_map = new ConcurrentHashMap<Integer, Communication>();
//    		tmp_map.put(taskGroupId, communication);
//    		taskGroupCommunicationMap.put(jobId, tmp_map);
//		}else {
//			taskGroupCommunicationMap.get(jobId).put(taskGroupId, communication);
//		}
//    }
//
//    public static Communication getJobCommunication(long jobId) {
//    	if (!taskGroupCommunicationMap.containsKey(jobId) 
//    			|| LocalJobCommunicationManager.getJobCommunication(jobId) == null) {
//			return null;
//		}
//        Communication communication = LocalJobCommunicationManager.getJobCommunication(jobId);
//        for (Communication taskGroupCommunication :
//                taskGroupCommunicationMap.get(jobId).values()) {
//            communication.mergeFrom(taskGroupCommunication);
//        }
//
//        return communication;
//    }
//
//    /**
//     * 采用获取taskGroupId后再获取对应communication的方式，
//     * 防止map遍历时修改，同时也防止对map key-value对的修改
//     *
//     * @return
//     */
//    public static Set<Integer> getTaskGroupIdSet(long jobId) {
//        return taskGroupCommunicationMap.get(jobId).keySet();
//    }
//
//    public static Communication getTaskGroupCommunication(long jobId, int taskGroupId) {
//        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");
//
//        return taskGroupCommunicationMap.get(jobId).get(taskGroupId);
//    }
//
//    public static void updateTaskGroupCommunication(long jobId,final int taskGroupId,
//                                                    final Communication communication) {
//        Validate.isTrue(taskGroupCommunicationMap.containsKey(
//        		jobId), String.format("taskGroupCommunicationMap中没有注册JobId[%d]的Communication，" +
//                "无法更新该taskGroup的信息", taskGroupId));
//        taskGroupCommunicationMap.get(jobId).put(taskGroupId, communication);
//    }
//
//    public static void clear() {
//        taskGroupCommunicationMap.clear();
//    }
//    
//    public static void cleanOneJob(long jobId) {
//        taskGroupCommunicationMap.remove(jobId);
//    }
//
//    public static Map<Integer, Communication> getTaskGroupCommunicationMap(long jobId) {
//        return taskGroupCommunicationMap.get(jobId);
//    }
//    
//    public static Set<Long> getAllJobId() {
//    	return taskGroupCommunicationMap.keySet();
//    }

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
	public Map<Integer, Communication> get(long jobId, int taskGroupId) {
		return taskCommunicationMap.get(jobId).get(taskGroupId);
	}

	@Override
	public void clearAll(long jobId) {
		taskCommunicationMap.remove(jobId);
	}

	@Override
	public void clearOne(long jobId, int taskGroupId) {
		taskCommunicationMap.get(jobId).remove(taskGroupId);
	}
}