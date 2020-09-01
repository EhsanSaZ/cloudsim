package org.mysim.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.mysim.Task;
import org.mysim.WorkflowDatacenterBroker;

import java.util.List;

public abstract class PlanningAlgorithmStrategy {
    public abstract void run();
    public abstract void ScheduleTasks(WorkflowDatacenterBroker broker,
                                       List<Task> readyTasks,
                                       List<Task> scheduledTasks,
                                       List<Container> newRequiredContainers,
                                       List<ContainerVm> newRequiredVms,
                                       List<Container> newRequiredContainersOnNewVms);
    public abstract List<Task> getScheduledTasksOnRunningContainers();
    public abstract void clear();
}
