package org.containerWorkflowsimDemo.scheduling;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;

import java.util.ArrayList;
import java.util.List;
//TODO EHSAN : ADD SUPPORT FOR CONTAINER
public abstract class ContainerBaseSchedulingAlgorithm implements ContainerSchedulingAlgorithmInterface{

    private List<? extends ContainerCloudlet> cloudletList;

    private List<? extends ContainerVm> vmList;

    private List<? extends Container> containerList;

    private List<ContainerCloudlet> scheduledList;

    public ContainerBaseSchedulingAlgorithm() {
        this.scheduledList = new ArrayList();
    }

    @Override
    public void setCloudletList(List list) {
        this.cloudletList = list;
    }

    @Override
    public void setVmList(List list) {
        this.vmList = new ArrayList(list);
    }

    public void setContainerList(List list) {
        this.containerList = new ArrayList(list);
    }


    @Override
    public List getCloudletList() {
        return this.cloudletList;
    }

    @Override
    public List getVmList() {
        return this.vmList;
    }

    public List getContainerList() {
        return this.containerList;
    }

    @Override
    public abstract void run() throws Exception;

    @Override
    public List getScheduledList() {
        return this.scheduledList;
    }

}
