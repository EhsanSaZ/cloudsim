package org.mysim.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.mysim.Task;
import org.mysim.WorkflowDatacenterBroker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MyPlanningAlgorithm extends PlanningAlgorithmStrategy{

    private List<Task> scheduledTasksOnRunningContainers;


    //    Comparator<Task> compareById = new Comparator<Task>() {
    //        @Override
    //        public int compare(Task t1, Task t2) {
    //            return Double.compare(t1.getSubDeadline(), t2.getSubDeadline());
    //        }
    //    };
    Comparator<Task> compareBySubDeadline = (t1, t2) -> Double.compare(t1.getSubDeadline(), t2.getSubDeadline());

    public MyPlanningAlgorithm(){
        setScheduledTasksOnRunningContainers(new ArrayList<>());
    }

    public void ScheduleTasks(WorkflowDatacenterBroker broker,
                              List<Task> readyTasks,
                              List<Task> scheduledTasks,
                              List<? extends Container> newRequiredContainers,
                              List<? extends ContainerVm> newRequiredVms,
                              List<? extends Container> newRequiredContainersOnNewVms) {

        readyTasks.sort(compareBySubDeadline);
        List<Task> waitQueue = new ArrayList<>();

    }

    @Override
    public void run() {

    }

    public void clear(){
        scheduledTasksOnRunningContainers.clear();
    }
    public List<Task> getScheduledTasksOnRunningContainers() {
        return scheduledTasksOnRunningContainers;
    }

    public void setScheduledTasksOnRunningContainers(List<Task> scheduledTasksOnRunningContainers) {
        this.scheduledTasksOnRunningContainers = scheduledTasksOnRunningContainers;
    }
}
