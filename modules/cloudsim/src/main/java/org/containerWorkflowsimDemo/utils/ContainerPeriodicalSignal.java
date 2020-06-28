package org.containerWorkflowsimDemo.utils;

public class ContainerPeriodicalSignal {

    private double period;

    private double upperbound;

    private double lowerbound;

    private double portion;

    private boolean direction;

    public ContainerPeriodicalSignal(double period, double upperbound, double lowerbound, double portion,
                            boolean direction) {
        this.lowerbound = lowerbound;
        this.upperbound = upperbound;
        this.period = period;
        this.portion = portion;
        this.direction = direction;
    }

    public ContainerPeriodicalSignal(double period, double upperbound, double lowerbound, double portion) {
        this(period, upperbound, lowerbound, portion, true);
    }

    public ContainerPeriodicalSignal(double period, double upperbound, double lowerbound) {
        this(period, upperbound, lowerbound, 0.5);
    }

    public double getCurrentSignal(double currentTime) {
        if (currentTime < 0.0) {
            return 0.0;
        }
        currentTime = currentTime % period;
        if (currentTime <= period * portion) {
            if (direction) {
                return upperbound;
            } else {
                return lowerbound;
            }
        } else {
            if (direction) {
                return lowerbound;
            } else {
                return upperbound;
            }
        }
    }

    public double getUpperBound() {
        return upperbound;
    }

    public double getLowerBound() {
        return lowerbound;
    }

    public double getPeriod() {
        return period;
    }

    public double getPortion() {
        return portion;
    }

    public boolean getDirection(){
        return direction;
    }
}
