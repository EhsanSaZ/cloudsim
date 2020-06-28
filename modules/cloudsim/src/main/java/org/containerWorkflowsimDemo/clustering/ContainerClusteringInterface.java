package org.containerWorkflowsimDemo.clustering;

import org.containerWorkflowsimDemo.ContainerFileItem;
import org.containerWorkflowsimDemo.ContainerJob;
import org.containerWorkflowsimDemo.ContainerTask;

import java.util.List;

public interface ContainerClusteringInterface {

    public void setTaskList(List<ContainerTask> list);


    public List<ContainerJob> getJobList();


    public List<ContainerTask> getTaskList();

    public void run();

    public List<ContainerFileItem> getTaskFiles();
}
