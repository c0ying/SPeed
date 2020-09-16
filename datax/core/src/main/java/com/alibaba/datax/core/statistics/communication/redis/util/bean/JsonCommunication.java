package com.alibaba.datax.core.statistics.communication.redis.util.bean;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class JsonCommunication extends BaseObject {

    /**
     * 所有的数值key-value对 *
     */
    private Map<String, Number> counter;

    /**
     * 运行状态 *
     */
    private Integer state;

    /**
     * 异常记录 *
     */
    private String throwable;

    /**
     * 记录的timestamp *
     */
    private long timestamp;

    private long startTimestamp;

    private long endTimestamp;

    /**
     * task给job的信息 *
     */
    private Map<String, List<String>> message;

    public Map<String, Number> getCounter() {
        return counter;
    }

    public void setCounter(Map<String, Number> counter) {
        this.counter = counter;
    }

    public String getThrowable() {
        return throwable;
    }

    public void setThrowable(String throwable) {
        this.throwable = throwable;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public Map<String, List<String>> getMessage() {
        return message;
    }

    public void setMessage(Map<String, List<String>> message) {
        this.message = message;
    }

    public Communication formate(){
        Communication communication = new Communication();
        this.getCounter().forEach((k,v) -> {
            if (v instanceof Double || v instanceof Float){
                communication.setDoubleCounter(k, (Double) v);
            }else if (v instanceof Long || v instanceof Integer){
                communication.setLongCounter(k, Long.valueOf(String.valueOf(v)));
            }
        });
        communication.setTimestamp(this.timestamp);
        communication.setStartTimestamp(this.startTimestamp);
        communication.setEndTimestamp(this.endTimestamp);
        this.getMessage().forEach((k,v) -> {
            v.forEach(message -> {
                communication.addMessage(k, message);
            });
        });
        communication.setState(State.fromValue(this.state));
        if (StringUtils.isNotBlank(this.getThrowable()))
        communication.addMessage("throwableMessage", this.getThrowable());
        return communication;
    }

    public static JsonCommunication build(Communication communication){
        JsonCommunication jsonCommunication = new JsonCommunication();
        jsonCommunication.setCounter(communication.getCounter());
        jsonCommunication.setState(communication.getState().value());
        jsonCommunication.setTimestamp(communication.getTimestamp());
        jsonCommunication.setStartTimestamp(communication.getStartTimestamp());
        jsonCommunication.setEndTimestamp(communication.getEndTimestamp());
        if (StringUtils.isNotBlank(communication.getThrowableMessage())){
            jsonCommunication.setThrowable(communication.getThrowableMessage());
        }
        jsonCommunication.setMessage(communication.getMessage());
        return jsonCommunication;
    }
}
