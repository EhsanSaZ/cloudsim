package org.containerWorkflowsimDemo.planning;

import java.util.List;

public interface ContainerPlanningAlgorithmInterface {
    /**
     * Sets the task list.
     */
    public void setTaskList(List list);

    /**
     * Sets the vm list.
     */
    public void setVmList(List list);

    public void setContainerList(List list);

    /**
     * Gets the task list.
     */
    public List getTaskList();

    /**
     * Gets the vm list. An algorithm must implement it
     */
    public List getVmList();

    public List getContainerList();

    /**
     * the main function.
     */
    public void run() throws Exception;
}
