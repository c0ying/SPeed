package com.alibaba.datax.core.statistics.communication;

import java.util.Map;
import java.util.Set;

public interface JobCommunicationManager {

    Communication registerJobCommunication(Long jobId);

    void clean(Long jobId);

    void registerTaskGroupCommunication(long jobId, int taskGroupId, Communication communication);

    Communication getJobCommunication(long jobId);

    Set<Integer> getTaskGroupIdSet(long jobId);

    Communication getTaskGroupCommunication(long jobId, int taskGroupId);

    Map<Integer, Communication> getTaskGroupCommunicationMap(long jobId);

    Set<Long> getAllJobId();

    void updateJobCommunication(long jobId, Communication communication);
}
