package org.mysim.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.mysim.Task;
import org.mysim.WorkflowDatacenterBroker;

import java.util.List;

public class MyPlanningAlgorithm extends PlanningAlgorithmStrategy{

    public void ScheduleTasks(WorkflowDatacenterBroker broker,
                              List<? extends Task> readyTasks,
                              List<? extends Container> newRequiredContainers,
                              List<? extends ContainerVm> newRequiredVms,
                              List<? extends Container> newRequiredContainersOnNewVms) {

    }

    @Override
    public void run() {

    }
}
