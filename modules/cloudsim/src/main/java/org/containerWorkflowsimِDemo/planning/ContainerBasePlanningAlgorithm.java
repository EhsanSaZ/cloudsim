package org.containerWorkflowsimِDemo.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerDatacenter;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.containerWorkflowsimِDemo.ContainerTask;


import java.util.ArrayList;
import java.util.List;

public abstract class ContainerBasePlanningAlgorithm implements ContainerPlanningAlgorithmInterface{

    /**
     * the task list.
     */
    private List<ContainerTask> tasktList;
    /**
     * the vm list.
     */
    private List<? extends ContainerVm> vmList;

    private List<? extends Container> ContainerList;

    private List<? extends ContainerDatacenter> datacenterList;

    public ContainerBasePlanningAlgorithm() {
    }

    @Override
    public void setTaskList(List list) {
        this.tasktList = list;
    }

    @Override
    public void setVmList(List list) {
        this.vmList = new ArrayList(list);
    }

    @Override
    public void setContainerList(List list) {
        this.ContainerList = new ArrayList(list);
    }

    @Override
    public List<ContainerTask> getTaskList() {
        return this.tasktList;
    }

    @Override
    public List getVmList() {
        return this.vmList;
    }
    @Override
    public List getContainerList() {
        return this.ContainerList;
    }

    public List getDatacenterList(){
        return this.datacenterList;
    }

    public void setDatacenterList(List list){
        this.datacenterList = list;
    }

    public abstract void run() throws Exception;
}
