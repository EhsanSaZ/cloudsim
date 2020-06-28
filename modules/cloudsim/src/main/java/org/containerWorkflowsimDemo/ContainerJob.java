package org.containerWorkflowsimDemo;


import java.util.ArrayList;
import java.util.List;

public class ContainerJob extends ContainerTask {
    /*
     * The list of tasks a job has. It is the only difference between Job and Task.
     */
    //TODO EHSAN: this need to be changed with respect to out definition of job set of tasks concurrently run together
    private List<ContainerTask> ContainerTaskList;

    public ContainerJob(final int jobId, final long jobLength, double peak_memory) {
        super(jobId, jobLength, peak_memory);
        this.ContainerTaskList = new ArrayList<>();
    }

    public List<ContainerTask> getContainerTaskList() {
        return this.ContainerTaskList;
    }

    public void setContainerTaskList(List list) {
        this.ContainerTaskList = list;
    }

    public void addTaskList(List list) {
        this.ContainerTaskList.addAll(list);
    }

    @Override
    public List getParentList() {
        return super.getParentList();
    }
}
