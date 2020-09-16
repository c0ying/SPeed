package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liqiang on 15/7/23.
 */
public class TaskMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMonitor.class);
    //@cyh
//    private static long EXPIRED_TIME = 172800 * 1000;
    private long expired_time = -1;

    private ConcurrentHashMap<Integer, TaskCommunication> tasks = new ConcurrentHashMap<Integer, TaskCommunication>();

    private TaskMonitor() {}

    private TaskMonitor(long expiredTime) {
        this.expired_time = expiredTime;
    }

    //@cyh 每个TaskGroup拥有独立的TaskMonitor
    public static TaskMonitor getInstance() {
        return new TaskMonitor();
    }

    public static TaskMonitor getInstance(long expiredTime) {
        return new TaskMonitor(expiredTime);
    }

    public void registerTask(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        if (this.expired_time > 0){
            tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication,this.expired_time));
        }else{
            tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
        }
    }

    public void removeTask(Integer taskid) {
        tasks.remove(taskid);
    }

    public void report(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        if (!tasks.containsKey(taskid)) {
            LOG.warn("unexpected: taskid({}) missed.", taskid);
            tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
        } else {
            tasks.get(taskid).report(communication);
        }
    }

    public TaskCommunication getTaskCommunication(Integer taskid) {
        return tasks.get(taskid);
    }


    public static class TaskCommunication {
        //@cyh,原值太大了，监控任务超时没效果
        //private static long EXPIRED_TIME = 1800 * 1000;
        private Integer taskid;
        //记录最后更新的communication
        private long lastAllReadRecords = -1;
        //只有第一次，或者统计变更时才会更新TS
        private long lastUpdateComunicationTS;
        private long ttl;
        //@cyh,原值太大了，监控任务超时没效果
        private long expireTime = 1800 * 1000;

        private TaskCommunication(Integer taskid, Communication communication) {
            this.taskid = taskid;
            lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
            ttl = System.currentTimeMillis();
            lastUpdateComunicationTS = ttl;
        }

        private TaskCommunication(Integer taskid, Communication communication, long expireTime) {
            this(taskid, communication);
            if (expireTime > 0){
                this.expireTime = expireTime;
            }
        }

        public void report(Communication communication) {

            ttl = System.currentTimeMillis();
            //采集的数量增长，则变更当前记录, 优先判断这个条件，因为目的是不卡住，而不是expired
            if (CommunicationTool.getTotalReadRecords(communication) > lastAllReadRecords) {
                lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
                lastUpdateComunicationTS = ttl;
            } else if (isExpired(lastUpdateComunicationTS)) {
                communication.setState(State.FAILED);
                communication.setTimestamp(ttl);
                communication.setThrowable(DataXException.asDataXException(CommonErrorCode.TASK_HUNG_EXPIRED,
                        String.format("task(%s) hung expired [allReadRecord(%s), elased(%s)]", taskid, lastAllReadRecords, (ttl - lastUpdateComunicationTS))));
            }


        }

        private boolean isExpired(long lastUpdateComunicationTS) {
            return System.currentTimeMillis() - lastUpdateComunicationTS > expireTime;
        }

        public Integer getTaskid() {
            return taskid;
        }

        public long getLastAllReadRecords() {
            return lastAllReadRecords;
        }

        public long getLastUpdateComunicationTS() {
            return lastUpdateComunicationTS;
        }

        public long getTtl() {
            return ttl;
        }
    }
}
