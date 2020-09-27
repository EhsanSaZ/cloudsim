package org.mysim.planning;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.CondorVM;
import org.mysim.Task;
import org.mysim.WorkflowDatacenterBroker;
import org.mysim.simschedulers.ContainerCloudletSchedulerSpaceShared;
import org.mysim.utils.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MWHBDCS_PlanningAlgorithm extends PlanningAlgorithmStrategy{

    private List<Task> scheduledTasksOnRunningContainers;
    // sort descending order
    Comparator<Task> compareByRank = (t1, t2) -> Double.compare(t2.getRank(), t1.getRank());

    public MWHBDCS_PlanningAlgorithm(){
        setScheduledTasksOnRunningContainers(new ArrayList<>());
    }

    public void ScheduleTasks(WorkflowDatacenterBroker broker,
                              List<Task> readyTasks,
                              List<Task> scheduledTasks,
                              List<Container> newRequiredContainers,
                              List<ContainerVm> newRequiredVms,
                              List<Container> newRequiredContainersOnNewVms){

        List<Task> toRemove = new ArrayList<>();
        List<Container> idleRunningContainerList = new ArrayList<>();
        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Collecting idle Running Containers");
        for (Container container: broker.getContainersCreatedList()){
            if(container.getWorkloadMips() == 0){
                idleRunningContainerList.add(container);
            }
        }
        if (idleRunningContainerList.size() > 0){
            Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Sending signal to destroy idle containers");
        }
        // collect and destroy all containers with workload 0
        for (Container container:idleRunningContainerList){
            broker.destroyContainer(container);
        }

        readyTasks.sort(compareByRank);
        for (Task task: readyTasks){
            // Run MWHBDCS on running containers as resources.
//            int index =  scheduleOnRunningContainers(idleRunningContainerList, task);
//            if (index !=-1){
//
//            }else{
//
//            }
            int selectedType = -1;
            List <Integer> affordableTypes = new ArrayList<>();
            double minCost = Double.MAX_VALUE;
            double minFinishTime = Double.MAX_VALUE;
            for (int j = 0; j < Parameters.VM_TYPES_NUMBERS; j++){
                double executionTime = (task.getTaskTotalLength() / (Parameters.VM_PES[j] * Parameters.VM_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                double estimatedCost =Parameters.COST[j] * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
                minFinishTime = Math.min(minFinishTime, executionTime);
                minCost = Math.min(minCost, estimatedCost);
                if (estimatedCost <= task.getSubBudget()){
                    affordableTypes.add(j);
                }
            }
            if (affordableTypes.size() != 0){
                double maxBFFactor = -Double.MAX_VALUE;
                int selectedTypeIndex = -1;
                for (int type : affordableTypes){
                    double currentBFfactor = calculateBFFActorOnVMType(type, task, minFinishTime, minCost);
                    if (currentBFfactor > maxBFFactor){
                        maxBFFactor = currentBFfactor;
                        selectedTypeIndex = type;
                    }
                }
                selectedType = selectedTypeIndex;
            }else {
                if (task.getSubDeadline() < minFinishTime){
                    int selectedTypeIndex = -1;
                    double smallestFinishTime = Double.MAX_VALUE;
                    for (int j = 0; j < Parameters.VM_TYPES_NUMBERS; j++){
                        double executionTime = (task.getTaskTotalLength() / (Parameters.VM_PES[j] * Parameters.VM_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                        if (executionTime < smallestFinishTime){
                            selectedTypeIndex = j;
                        }
                    }
                    selectedType = selectedTypeIndex;
                }else {
                    int selectedTypeIndex = -1;
                    double smallestCost = Double.MAX_VALUE;
                    for (int j = 0; j < Parameters.VM_TYPES_NUMBERS; j++){
                        double executionTime = (task.getTaskTotalLength() / (Parameters.VM_PES[j] * Parameters.VM_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                        double estimatedCost =Parameters.COST[j] * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
                        if (estimatedCost < smallestCost){
                            selectedTypeIndex = j;
                        }
                    }
                    selectedType = selectedTypeIndex;
                }
            }
            // create container corresponding to selected vm type
            // check if it is possible to run on an already running vm
            // if no create new vm for this container...
            Container newContainer = new Container(IDs.pollId(Container.class), broker.getId(),Parameters.CONTAINER_MIPS[0],
                    Parameters.VM_PES[selectedType], (int)Parameters.VM_RAM[selectedType], (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE,
                    "Xen",new ContainerCloudletSchedulerSpaceShared(),Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);

            boolean isScheduled = false;
            for (ContainerVm vm : broker.getVmsCreatedList()){
                CondorVM castedVm = (CondorVM) vm;
                if (castedVm.getAvailablePeNumbersForSchedule() == newContainer.getNumberOfPes() && castedVm.getAvailableRamForSchedule() == newContainer.getRam()){
                    //schedule container on this vm and go for next task
                    castedVm.setAvailablePeNumbersForSchedule(castedVm.getAvailablePeNumbersForSchedule() - newContainer.getNumberOfPes());
                    castedVm.setAvailableRamForSchedule(castedVm.getAvailableRamForSchedule() - newContainer.getRam());
                    castedVm.setAvailableSizeForSchedule(castedVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);

                    newContainer.setVm(vm);

                    task.setVmId(vm.getId());
                    task.setContainerId(newContainer.getId());
                    task.setNumberOfPes(newContainer.getNumberOfPes());
                    task.updateCoudletLength(task.getNumberOfPes());

                    newRequiredContainers.add(newContainer);
                    scheduledTasks.add(task);
                    toRemove.add(task);
                    isScheduled = true;
                    Log.printConcatLine(CloudSim.clock(), ": MWHBDCS: ",
                            "Task #", task.getCloudletId(), " scheduled to run on VM #", castedVm.getId(), " and Container #",
                            newContainer.getId());
                    break;
                }
            }
            if (!isScheduled){
                // create new vm for this container and schedule it
                ArrayList<ContainerPe> peList = new ArrayList<ContainerPe>();
                for (int p = 0; p < Parameters.VM_PES[selectedType]; ++p){
                    peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double) Parameters.VM_MIPS[selectedType])));
                }
                CondorVM newVm = new CondorVM(IDs.pollId(ContainerVm.class), broker.getId(), Parameters.VM_MIPS[selectedType],
                        Parameters.VM_RAM[selectedType], Parameters.VM_BW, Parameters.VM_SIZE, "Xen",
                        new ContainerSchedulerTimeSharedOverSubscription(peList),
                        new ContainerRamProvisionerSimple(Parameters.VM_RAM[selectedType]),
                        new ContainerBwProvisionerSimple(Parameters.VM_BW),
                        peList, Parameters.CONTAINER_VM_SCHEDULING_INTERVAL,
                        Parameters.COST[selectedType], Parameters.COST_PER_MEM[selectedType],
                        Parameters.COST_PER_STORAGE[selectedType], Parameters.COST_PER_BW[selectedType]);

                newVm.setAvailablePeNumbersForSchedule(newVm.getAvailablePeNumbersForSchedule() - newContainer.getNumberOfPes());
                newVm.setAvailableRamForSchedule(newVm.getAvailableRamForSchedule() - newContainer.getRam());
                newVm.setAvailableSizeForSchedule(newVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);

                newContainer.setVm(newVm);

                task.setVmId(newVm.getId());
                task.setContainerId(newContainer.getId());
                task.setNumberOfPes(newContainer.getNumberOfPes());
                task.updateCoudletLength(task.getNumberOfPes());


                newRequiredVms.add(newVm);
                newRequiredContainersOnNewVms.add(newContainer);
                scheduledTasks.add(task);
                toRemove.add(task);
                Log.printConcatLine(CloudSim.clock(), ": MWHBDCS: ",
                        "Task #", task.getCloudletId(), " scheduled to run on VM #", newVm.getId(), " and Container #",
                        newContainer.getId());
            }
        }
        idleRunningContainerList.clear();
        readyTasks.removeAll(toRemove);
    }


    public int scheduleOnRunningContainers( List <Container> containerList, Task task){
        int index = -1;
        List <Container> affordableContainers = new ArrayList<>();
        double minCost = Double.MAX_VALUE;
        double minFinishTime = Double.MAX_VALUE;
        for (Container container: containerList){
            CondorVM castedVm =  (CondorVM) container.getVm();
            double executionTime = (task.getTaskTotalLength() / (container.getNumberOfPes() * Parameters.CONTAINER_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
            double estimatedCost = castedVm.getCost() * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
            minFinishTime = Math.min(minFinishTime, executionTime);
            minCost = Math.min(minCost, estimatedCost);
            if (estimatedCost <= task.getSubBudget()){
                affordableContainers.add(container);
            }
        }
        if (affordableContainers.size() != 0){
            double maxBFFactore = -Double.MAX_VALUE;
            int selectedContainerIndex = -1;
            for (Container container : affordableContainers){
                    double currentBFFactor = calculateBFFActor(container, task, minFinishTime, minCost);
                    if ( currentBFFactor > maxBFFactore){
                        maxBFFactore = currentBFFactor;
                        selectedContainerIndex = affordableContainers.indexOf(container);
                    }
            }
            index = containerList.indexOf(affordableContainers.get(selectedContainerIndex));
        }else{
            if (task.getSubDeadline() < minFinishTime){
                int selectedContainerIndex = -1;
                double smallestFinishTime = Double.MAX_VALUE;
                for (Container container: containerList){
                    double executionTime = (task.getTaskTotalLength() / (container.getNumberOfPes() * Parameters.CONTAINER_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                    if (executionTime < smallestFinishTime){
                        selectedContainerIndex = containerList.indexOf(container);
                    }
                }
                index = selectedContainerIndex;
            }else{
                int selectedContainerIndex = -1;
                double smallestCost = Double.MAX_VALUE;
                for (Container container: containerList){
                    CondorVM castedVm =  (CondorVM) container.getVm();
                    double executionTime = (task.getTaskTotalLength() / (container.getNumberOfPes() * Parameters.CONTAINER_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                    double estimatedCost = castedVm.getCost() * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
                    if (estimatedCost < smallestCost){
                        selectedContainerIndex = containerList.indexOf(container);
                    }
                }
                index = selectedContainerIndex;
            }
        }
        if (index != -1){
            containerList.get(index).setWorkloadMips(Parameters.CONTAINER_MIPS[0]);
            task.setContainerId(containerList.get(index).getId());
            task.setVmId(containerList.get(index).getVm().getId());
            task.setNumberOfPes(containerList.get(index).getNumberOfPes());
            task.updateCoudletLength(task.getNumberOfPes());
            getScheduledTasksOnRunningContainers().add(task);
        }
        return index;
    }

    public double calculateBFFActor(Container container, Task task, double minFinishtime, double minCost)   {
        CondorVM castedVm =  (CondorVM) container.getVm();
        double executionTime = (task.getTaskTotalLength() / (container.getNumberOfPes() * Parameters.CONTAINER_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
        double estimatedCost = castedVm.getCost() * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
        double timeF = executionTime * -1;
        double costF = 0;
        if (executionTime < task.getSubDeadline()){
            timeF = (task.getSubDeadline() - executionTime) / (task.getSubDeadline() - minFinishtime);
            costF = (task.getSubBudget() - estimatedCost) / (task.getSubBudget() - minCost);
        }
        return timeF + costF;
    }

    public double calculateBFFActorOnVMType(int type, Task task, double minFinishtime, double minCost){
        double executionTime = (task.getTaskTotalLength() / (Parameters.VM_PES[type] * Parameters.VM_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
        double estimatedCost = Parameters.COST[type] * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
        double timeF = executionTime * -1;
        double costF = 0;
        if (executionTime < task.getSubDeadline()){
            timeF = (task.getSubDeadline() - executionTime) / (task.getSubDeadline() - minFinishtime);
            costF = (task.getSubBudget() - estimatedCost) / (task.getSubBudget() - minCost);
        }
        return timeF + costF;
    }

    @Override
    public void run() {

    }
    public void clear(){
        scheduledTasksOnRunningContainers.clear();
    }

    //-----------------------setter and getter
    public List<Task> getScheduledTasksOnRunningContainers() {
        return scheduledTasksOnRunningContainers;
    }

    public void setScheduledTasksOnRunningContainers(List<Task> scheduledTasksOnRunningContainers) {
        this.scheduledTasksOnRunningContainers = scheduledTasksOnRunningContainers;
    }
}
