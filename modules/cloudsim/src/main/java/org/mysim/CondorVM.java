package org.mysim;

import org.cloudbus.cloudsim.VmStateHistoryEntry;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.mysim.utils.MySimTags;
import org.mysim.utils.VmStateEntry;
//import org.workflowsim.WorkflowSimTags;

import java.util.LinkedList;
import java.util.List;

public class CondorVM extends PowerContainerVm {
    private int state;
    private final List<VmStateEntry> busyStateHistory = new LinkedList<VmStateEntry>();

    private double costPerMem = 0.0;
    private double costPerBW = 0.0;
    private double costPerStorage = 0.0;
    private double cost = 0.0;

    private double leaseTime = -1;
    private double releaseTime = -1;

    public CondorVM(final int id, final int userId, final double mips, final float ram, final long bw, final long size, final String vmm,
                    final ContainerScheduler containerScheduler, final ContainerRamProvisioner containerRamProvisioner, final ContainerBwProvisioner containerBwProvisioner,
                    List<? extends ContainerPe> peList, final double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner,
                peList, schedulingInterval);
        setState(MySimTags.VM_STATUS_IDLE);
    }

    public CondorVM(final int id, final int userId, final double mips, final float ram, final long bw, final long size, final String vmm,
                    final ContainerScheduler containerScheduler, final ContainerRamProvisioner containerRamProvisioner, final ContainerBwProvisioner containerBwProvisioner,
                    List<? extends ContainerPe> peList, final double schedulingInterval,
                    double cost, double costPerMem, double costPerStorage, double costPerBW) {
        this(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner,
            peList, schedulingInterval);
        setCost(cost);
        setCostPerMem(costPerMem);
        setCostPerStorage(costPerStorage);
        setCostPerBW(costPerBW);
    }

    public void addBusyStateHistory( double time, int state){
        VmStateEntry newState = new VmStateEntry(time, state);
        if (!getBusyStateHistory().isEmpty()){
            VmStateEntry previousState = getBusyStateHistory().get(getBusyStateHistory().size() - 1);
            if (previousState.getTime() == time) {
                getBusyStateHistory().set(getBusyStateHistory().size() - 1, newState);
                return;
            }
        }
        getBusyStateHistory().add(newState);
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public List<VmStateEntry> getBusyStateHistory() {
        return busyStateHistory;
    }

    public double getCostPerMem() {
        return costPerMem;
    }

    public void setCostPerMem(double costPerMem) {
        this.costPerMem = costPerMem;
    }

    public double getCostPerBW() {
        return costPerBW;
    }

    public void setCostPerBW(double costPerBW) {
        this.costPerBW = costPerBW;
    }

    public double getCostPerStorage() {
        return costPerStorage;
    }

    public void setCostPerStorage(double costPerStorage) {
        this.costPerStorage = costPerStorage;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getLeaseTime() {
        return leaseTime;
    }

    public double getReleaseTime() {
        return releaseTime;
    }

    public void setLeaseTime(double leaseTime) {
        this.leaseTime = leaseTime;
    }

    public void setReleaseTime(double releaseTime) {
        this.releaseTime = releaseTime;
    }
}
