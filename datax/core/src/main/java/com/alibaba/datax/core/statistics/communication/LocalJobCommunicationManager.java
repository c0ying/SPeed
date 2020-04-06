package com.alibaba.datax.core.statistics.communication;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Validate;

import com.alibaba.datax.dataxservice.face.domain.enums.State;

public class LocalJobCommunicationManager {

	private static Map<Long, Communication> JOB_COMMUNICATION = new ConcurrentHashMap<>();
	
	private static Map<Long, Map<Integer, Communication>> taskGroupCommunicationMap =
            new ConcurrentHashMap<Long, Map<Integer, Communication>>();
	
	private static LocalJobCommunicationManager localJobCommunicationManager = new LocalJobCommunicationManager();
	
	private LocalJobCommunicationManager() {};
	
	public static LocalJobCommunicationManager getInstance() {
		return localJobCommunicationManager;
	}
	
	public Communication registerJobCommunication(Long jobId) {
		if (!JOB_COMMUNICATION.containsKey(jobId)) {
			Communication communication = new Communication();
			communication.setStartTimestamp(System.currentTimeMillis());
        	communication.setState(State.SUBMITTING);
			JOB_COMMUNICATION.put(jobId, communication);
			return communication;
		}
		return null;
	}
	
	public void clean(Long jobId) {
		JOB_COMMUNICATION.remove(jobId);
		taskGroupCommunicationMap.remove(jobId);
	}
	
	public void registerTaskGroupCommunication(long jobId,int taskGroupId, Communication communication) {
    	if (!taskGroupCommunicationMap.containsKey(jobId)) {
    		Map<Integer, Communication> tmp_map = new ConcurrentHashMap<Integer, Communication>();
    		tmp_map.put(taskGroupId, communication);
    		taskGroupCommunicationMap.put(jobId, tmp_map);
		}else {
			taskGroupCommunicationMap.get(jobId).put(taskGroupId, communication);
		}
    }

    public Communication getJobCommunication(long jobId) {
//    	if (!JOB_COMMUNICATION.containsKey(jobId)) {
//			return null;
//		}
//        Communication communication = JOB_COMMUNICATION.get(jobId).clone();
//        if (taskGroupCommunicationMap.containsKey(jobId)) {
//    		for (Communication taskGroupCommunication : taskGroupCommunicationMap.get(jobId).values()) {
//    			communication.mergeFrom(taskGroupCommunication);
//    		}
//        }
//
//        return communication;
    	return JOB_COMMUNICATION.get(jobId);
    }
    
    /**
     * 采用获取taskGroupId后再获取对应communication的方式，
     * 防止map遍历时修改，同时也防止对map key-value对的修改
     *
     * @return
     */
    public Set<Integer> getTaskGroupIdSet(long jobId) {
        return taskGroupCommunicationMap.get(jobId).keySet();
    }

    public Communication getTaskGroupCommunication(long jobId, int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");

        return taskGroupCommunicationMap.get(jobId).get(taskGroupId);
    }

    public void updateTaskGroupCommunication(long jobId,final int taskGroupId,
                                                    final Communication communication) {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(
        		jobId), String.format("taskGroupCommunicationMap中没有注册JobId[%d]的Communication，" +
                "无法更新该taskGroup的信息", taskGroupId));
        if (taskGroupCommunicationMap.containsKey(jobId)) {
        	Communication oldCommunication = getTaskGroupCommunication(jobId, taskGroupId);
        	if (communication.getStartTimestamp() <= 0 && oldCommunication.getStartTimestamp() > 0) {
        		communication.setStartTimestamp(oldCommunication.getStartTimestamp());
			}
        	if (communication.getEndTimestamp() <= 0 && oldCommunication.getEndTimestamp() > 0) {
        		communication.setEndTimestamp(oldCommunication.getEndTimestamp());
			}
        	taskGroupCommunicationMap.get(jobId).put(taskGroupId, communication);
		}
    }
    
    public void updateJobCommunication(long jobId, Communication communication) {
    	Validate.isTrue(JOB_COMMUNICATION.containsKey(jobId), 
    			String.format("没有注册JobId[%d]的Communication，无法更新该Job的信息", jobId));
    	if (JOB_COMMUNICATION.containsKey(jobId)) {
    		Communication oldCommunication = getJobCommunication(jobId);
    		if (communication.getStartTimestamp() <= 0 && oldCommunication.getStartTimestamp() > 0) {
        		communication.setStartTimestamp(oldCommunication.getStartTimestamp());
			}
        	if (communication.getEndTimestamp() <= 0 && oldCommunication.getEndTimestamp() > 0) {
        		communication.setEndTimestamp(oldCommunication.getEndTimestamp());
			}
    		JOB_COMMUNICATION.put(jobId, communication);
		}
    }

    public void clear(long jobId) {
        taskGroupCommunicationMap.remove(jobId);
        JOB_COMMUNICATION.remove(jobId);
    }

    public Map<Integer, Communication> getTaskGroupCommunicationMap(long jobId) {
        return taskGroupCommunicationMap.get(jobId);
    }
    
    public Set<Long> getAllJobId() {
    	return JOB_COMMUNICATION.keySet();
    }
}
