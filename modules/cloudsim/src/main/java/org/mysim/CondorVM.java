package org.mysim;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.mysim.utils.MySimTags;
//import org.workflowsim.WorkflowSimTags;

import java.util.List;

public class CondorVM extends PowerContainerVm {
    private int state;

    private double costPerMem = 0.0;
    private double costPerBW = 0.0;
    private double costPerStorage = 0.0;
    private double cost = 0.0;

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

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
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
}
