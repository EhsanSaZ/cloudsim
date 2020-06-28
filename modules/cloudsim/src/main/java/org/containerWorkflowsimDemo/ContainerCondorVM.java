package org.containerWorkflowsimDemo;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.workflowsim.WorkflowSimTags;

import java.util.List;

public class ContainerCondorVM extends ContainerVm {

    /*
     * The state of a vm. It should be either WorkflowSimTags.VM_STATUS_IDLE
     * or VM_STATUS_READY (not used in workflowsim) or VM_STATUS_BUSY
     */
    private int state;

    private double costPerMem = 0.0;

    private double costPerBW = 0.0;

    private double costPerStorage = 0.0;

    private double cost = 0.0;
    public ContainerCondorVM(int id, int userId, double mips, float ram, long bw, long size, String vmm,
                             ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner,
                             List<? extends ContainerPe> peList) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList);
        /*
         * At the beginning all vm status is idle.
         */
        //TODO EHSAN : SHOULD BE CHECKED
        setState(WorkflowSimTags.VM_STATUS_IDLE);
    }
    public ContainerCondorVM(int id, int userId, double mips, float ram, long bw, long size, String vmm,
                             double cost, double costPerMem, double costPerStorage, double costPerBW,
                             ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner,
                             List<? extends ContainerPe> peList) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList);
        this.cost = cost;
        this.costPerBW = costPerBW;
        this.costPerMem = costPerMem;
        this.costPerStorage = costPerStorage;
        /*
         * At the beginning all vm status is idle.
         */
        setState(WorkflowSimTags.VM_STATUS_IDLE);
    }
    public double getCost() {
        return this.cost;
    }

    public double getCostPerBW() {
        return this.costPerBW;
    }

    public double getCostPerStorage() {
        return this.costPerStorage;
    }

    public double getCostPerMem() {
        return this.costPerMem;
    }


    public final void setState(int tag) {
        this.state = tag;
    }
    public final int getState() {
        return this.state;
    }

}
