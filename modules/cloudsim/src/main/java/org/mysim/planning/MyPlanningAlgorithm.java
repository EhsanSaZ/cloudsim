package org.mysim.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.mysim.Task;

import java.util.List;

public class MyPlanningAlgorithm extends PlanningAlgorithmStrategy{

    public void ScheduleTasks(List<? extends Task> readyTasks, List<? extends ContainerVm> runningVms,
                    List<? extends Container> newRequiredContainers,
                    List<? extends ContainerVm> newRequiredVms,
                    List<? extends Container> newRequiredContainersOnNewVms) {

    }

    @Override
    public void run() {

    }
}
