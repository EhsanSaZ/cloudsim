package org.mysim.utils;

public class VmStateEntry {
    /** The time. */
    private double time;
    private int state;

    public VmStateEntry(double time, int state) {
        this.time = time;
        this.state = state;
    }

    public double getTime() {
        return time;
    }

    public void setTime(double time) {
        this.time = time;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
