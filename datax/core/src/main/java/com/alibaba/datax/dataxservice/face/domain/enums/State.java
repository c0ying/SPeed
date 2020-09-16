package com.alibaba.datax.dataxservice.face.domain.enums;

public enum State implements EnumVal {

    SUBMITTING(10),
    WAITING(20),
    RUNNING(30),
    KILLING(40),
    KILLED(50),
    FAILED(60),
    SUCCEEDED(70);


    /* 一定会被初始化的 */
    int value;

    State(int value) {
        this.value = value;
    }

    @Override
    public int value() {
        return value;
    }

    public static State fromValue(int value){
        for (State state : State.values()) {
            if (state.value == value){
                return state;
            }
        }
        throw new IllegalArgumentException("Unkown value");
    }


    public boolean isFinished() {
        return this == KILLED || this == FAILED || this == SUCCEEDED;
    }

    public boolean isRunning() {
        return !isFinished();
    }

}