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
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.WorkflowDatacenterBroker;
import org.mysim.simschedulers.ContainerCloudletSchedulerSpaceShared;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;

import java.util.*;
import java.util.Map.Entry;

public class EPSMPlanningAlgorithm extends PlanningAlgorithmStrategy{

    private List<Task> scheduledTasksOnRunningContainers;
    //    Comparator<Task> compareById = new Comparator<Task>() {
    //        @Override
    //        public int compare(Task t1, Task t2) {
    //            return Double.compare(t1.getSubDeadline(), t2.getSubDeadline());
    //        }
    //    };
    // sort ascending order
    Comparator<Task> compareBySubDeadline = (t1, t2) -> Double.compare(t1.getSubDeadline(), t2.getSubDeadline());
    // sort ascending order
    Comparator<Task> compareByDepth = (t1, t2) -> Double.compare(t1.getDepth(), t2.getDepth());
    // sort ascending order
    Comparator<Entry<String, Double>> valueComparator = new Comparator<Entry<String,Double>>() {
        @Override
        public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
            return e1.getValue().compareTo( e2.getValue()); }
    };
    private Random rd = new Random();

    public EPSMPlanningAlgorithm(){
        setScheduledTasksOnRunningContainers(new ArrayList<>());
    }

    public void ScheduleTasks(WorkflowDatacenterBroker broker,
                              List<Task> readyTasks,
                              List<Task> scheduledTasks,
                              List<Container> newRequiredContainers,
                              List<ContainerVm> newRequiredVms,
                              List<Container> newRequiredContainersOnNewVms) {
        Log.printConcatLine(CloudSim.clock(), ": EPSM: Start scheduling Ready task Queue with ",readyTasks.size(), "tasks");
        List<Task> toRemove = new ArrayList<>();
//        List<Container> idleRunningContainerList = new ArrayList<>();
        Map<ContainerVm, Container> VMToIdleContainerMap = new HashMap<>();

        readyTasks.sort(compareBySubDeadline);

        Log.printConcatLine(CloudSim.clock(), ": EPSM: Collecting idle Running Containers");
        for (Container container: broker.getContainersCreatedList()){
            if(container.getWorkloadMips() == 0){
//                idleRunningContainerList.add(container);
                VMToIdleContainerMap.put(container.getVm(), container);
            }
        }
        for (Task task: readyTasks){
            List <ContainerVm> allIdleVm= new ArrayList<>(VMToIdleContainerMap.keySet());
            for (ContainerVm vm:broker.getVmsCreatedList()){
                CondorVM castedVm = (CondorVM) vm;
                assert castedVm != null;
                if (castedVm.getAvailablePeNumbersForSchedule() == castedVm.getNumberOfPes() && castedVm.getAvailableRamForSchedule() == castedVm.getRam()){
                    allIdleVm.add(vm);
                }
            }

            List <ContainerVm> idleVmWithInput = getVmsWithInputData(allIdleVm, task);
            if (idleVmWithInput.size() > 0){
                ContainerVm provisionedVm = null;
                boolean scheduledOnNewVm = false;
                provisionedVm = searchForAppropriateVm(idleVmWithInput, VMToIdleContainerMap, task);
                if (provisionedVm == null){
                    allIdleVm.removeAll(idleVmWithInput);
                    List <ContainerVm> idleWithContainer = new ArrayList<>();
                    for(ContainerVm vm :allIdleVm){
                        if(VMToIdleContainerMap.containsKey(vm)){
                            idleWithContainer.add(vm);
                        }
                    }
                    provisionedVm = searchForAppropriateVm(idleWithContainer, VMToIdleContainerMap, task);
                    if (provisionedVm==null){
                        allIdleVm.removeAll(idleWithContainer);
                        provisionedVm = searchForAppropriateVm(allIdleVm, VMToIdleContainerMap, task);
                        if (provisionedVm == null){
                            boolean delayIsPossible = checkDelayPossibility(task);
                            if(!delayIsPossible){
                                scheduledOnNewVm = true;
                                provisionedVm = provisionNewVmForTask(task, broker.getId());
                            }
                            else {
                                Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Task# ", task.getCloudletId() , " is delayed");
                                double remainingTimeToDeadline = task.getSubDeadline() - (CloudSim.clock() + Parameters.R_T_Q_SCHEDULING_INTERVAL);
                                task.setSubDeadline(remainingTimeToDeadline);
                            }
                        }
                    }
                }
                if (provisionedVm != null){
                    // if it is a with with container .. remove from map and list
                    Container newContainer = null;
                    CondorVM castedVm =  (CondorVM) provisionedVm;
                    if(!VMToIdleContainerMap.containsKey(provisionedVm)){
                        newContainer = new Container(IDs.pollId(Container.class),
                                broker.getId(),Parameters.CONTAINER_MIPS[0],
                                provisionedVm.getNumberOfPes(), (int) provisionedVm.getRam(), (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE,
                                "Xen",new ContainerCloudletSchedulerSpaceShared(),Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
                        newContainer.setVm(provisionedVm);
                        if (scheduledOnNewVm){
                            newRequiredVms.add(provisionedVm);
                            newRequiredContainersOnNewVms.add(newContainer);
                        }else{
                            newRequiredContainers.add(newContainer);
                        }

                        castedVm.setAvailablePeNumbersForSchedule(castedVm.getAvailablePeNumbersForSchedule() - newContainer.getNumberOfPes());
                        castedVm.setAvailableRamForSchedule(castedVm.getAvailableRamForSchedule() - newContainer.getRam());
                        castedVm.setAvailableSizeForSchedule(castedVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);

                    }else {
                        newContainer = VMToIdleContainerMap.get(provisionedVm);
                        newContainer.setWorkloadMips(Parameters.CONTAINER_MIPS[0]);
                        getScheduledTasksOnRunningContainers().add(task);
                        VMToIdleContainerMap.remove(provisionedVm);
                    }

                    task.setVmId(provisionedVm.getId());
                    task.setContainerId(newContainer.getId());
                    task.setNumberOfPes(newContainer.getNumberOfPes());
                    // because actual cloudlet length in Res for execution is lenght per PE
                    task.updateCoudletLength(task.getNumberOfPes());
                    Log.printConcatLine(CloudSim.clock(), ": EPSM: ",
                            "Task #", task.getCloudletId(), " scheduled to run on VM #", castedVm.getId(), " and Container #",
                            newContainer.getId());
                    scheduledTasks.add(task);
                    toRemove.add(task);
                }
            }else{
                // when there os no idle VM same logic like part1. check for delay if it is not possible provision a new vm
                boolean delayIsPossible = checkDelayPossibility(task);
                if(!delayIsPossible){
                    ContainerVm provisionedVm = provisionNewVmForTask(task, broker.getId());
                    Container newContainer = new Container(IDs.pollId(Container.class),
                            broker.getId(),Parameters.CONTAINER_MIPS[0],
                            provisionedVm.getNumberOfPes(), (int) provisionedVm.getRam(), (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE,
                            "Xen",new ContainerCloudletSchedulerSpaceShared(),Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);

                    newContainer.setVm(provisionedVm);
                    newRequiredVms.add(provisionedVm);
                    newRequiredContainersOnNewVms.add(newContainer);
                    CondorVM castedVm =  (CondorVM) provisionedVm;
                    castedVm.setAvailablePeNumbersForSchedule(castedVm.getAvailablePeNumbersForSchedule() - newContainer.getNumberOfPes());
                    castedVm.setAvailableRamForSchedule(castedVm.getAvailableRamForSchedule() - newContainer.getRam());
                    castedVm.setAvailableSizeForSchedule(castedVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);
                    task.setVmId(provisionedVm.getId());
                    task.setContainerId(newContainer.getId());
                    task.setNumberOfPes(newContainer.getNumberOfPes());
                    // because actual cloudlet length in Res for execution is lenght per PE
                    task.updateCoudletLength(task.getNumberOfPes());
                    Log.printConcatLine(CloudSim.clock(), ": EPSM: ",
                            "Task #", task.getCloudletId(), " scheduled to run on VM #", castedVm.getId(), " and Container #",
                            newContainer.getId());
                    scheduledTasks.add(task);
                    toRemove.add(task);
                } else {
                    Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Task# ", task.getCloudletId() , " is delayed");
                    double remainingTimeToDeadline = task.getSubDeadline() - (CloudSim.clock() + Parameters.R_T_Q_SCHEDULING_INTERVAL);
                    task.setSubDeadline(remainingTimeToDeadline);
                }
            }
        }
        // Try to schedule tasks in ready Queue on already running resources (by broker)
        if (VMToIdleContainerMap.values().size() > 0){
            Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Sending signal to destroy idle containers");
        }
        // collect and destroy all containers with workload 0
        for (Container container:VMToIdleContainerMap.values()){
            broker.destroyContainer(container);
        }
        VMToIdleContainerMap.clear();
        // remove all scheduled tasks from ready task list
        readyTasks.removeAll(toRemove);
    }

    public boolean checkDelayPossibility(Task task){
        double cheapestCost = Double.MAX_VALUE;
        int cheapestType = 0;
        for (int type= 0; type < Parameters.VM_TYPES_NUMBERS; type++){
            double estimatedExecutionTime = Parameters.CONTAINER_PROVISIONING_DELAY
                    + (task.getTaskTotalLength() / (Parameters.VM_PES[type] * Parameters.VM_MIPS[0]))
                    + task.getTransferTime(Parameters.VM_BW);
            estimatedExecutionTime += Parameters.CONTAINER_PROVISIONING_DELAY;
            double estimatedCost =  Parameters.COST[type] * Math.ceil( estimatedExecutionTime / Parameters.BILLING_PERIOD);
            if (estimatedCost <cheapestCost){
                cheapestType = type;
            }
        }
        double estimatedExecutionTime = Parameters.CONTAINER_PROVISIONING_DELAY
                + (task.getTaskTotalLength() / (Parameters.VM_PES[cheapestType] * Parameters.VM_MIPS[0]))
                + task.getTransferTime(Parameters.VM_BW);;
        double spareTime = task.getSubDeadline()
                - (CloudSim.clock()  + estimatedExecutionTime + Parameters.R_T_Q_SCHEDULING_INTERVAL);
//        return !(spareTime <= 0);
        return (spareTime > 0);
    }
    public ContainerVm provisionNewVmForTask(Task task, int brokerID){
        double minCost = Double.MAX_VALUE;
        int chosenTypeIndex = -1;
        for (int vmType=0; vmType < Parameters.VM_TYPES_NUMBERS; vmType++){
            double executionTime = Parameters.CONTAINER_PROVISIONING_DELAY
                    + (task.getTaskTotalLength() / (Parameters.VM_PES[vmType] * Parameters.VM_MIPS[0]))
                    + task.getTransferTime(Parameters.VM_BW);
            double estimatedCost =  Parameters.COST[vmType] * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
            if(executionTime < task.getSubDeadline() && estimatedCost < minCost){
                chosenTypeIndex = vmType;
            }
        }
        if (chosenTypeIndex == -1){
            chosenTypeIndex = Parameters.VM_TYPES_NUMBERS -1;
        }
        ArrayList<ContainerPe> peList = new ArrayList<ContainerPe>();
        for (int p = 0; p < Parameters.VM_PES[chosenTypeIndex]; ++p){
            peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double) Parameters.VM_MIPS[chosenTypeIndex])));
        }
        CondorVM newVm = new CondorVM(IDs.pollId(ContainerVm.class), brokerID, Parameters.VM_MIPS[chosenTypeIndex],
                Parameters.VM_RAM[chosenTypeIndex], Parameters.VM_BW, Parameters.VM_SIZE, "Xen",
                new ContainerSchedulerTimeSharedOverSubscription(peList),
                new ContainerRamProvisionerSimple(Parameters.VM_RAM[chosenTypeIndex]),
                new ContainerBwProvisionerSimple(Parameters.VM_BW),
                peList, Parameters.CONTAINER_VM_SCHEDULING_INTERVAL,
                Parameters.COST[chosenTypeIndex], Parameters.COST_PER_MEM[chosenTypeIndex],
                Parameters.COST_PER_STORAGE[chosenTypeIndex], Parameters.COST_PER_BW[chosenTypeIndex]);

        return  newVm;
    }
    public ContainerVm searchForAppropriateVm (List<ContainerVm> vmList, Map<ContainerVm, Container> VMToIdleContainerMap,Task task){
        ContainerVm chosenVM = null;
        double minCost = Double.MAX_VALUE;
        for (ContainerVm vm: vmList){
            CondorVM castedVm = (CondorVM) vm;
            double estimatedExecutionTime = (task.getTaskTotalLength() / (vm.getNumberOfPes() * Parameters.VM_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);;
            if (!VMToIdleContainerMap.containsKey(vm)){
                estimatedExecutionTime += Parameters.CONTAINER_PROVISIONING_DELAY;
            }
            double estimatedCost =  castedVm.getCost() * Math.ceil( estimatedExecutionTime / Parameters.BILLING_PERIOD);

            if(estimatedExecutionTime < task.getSubDeadline() && estimatedCost < minCost){
                chosenVM = vm;
                // T ODO update container and Vm and task
            }
        }
        return chosenVM;
    }
    public List<ContainerVm> getVmsWithInputData( List<ContainerVm> vmList, Task task){
        List<ContainerVm> filteredVms = new ArrayList();
        for( ContainerVm vm: vmList){
            for (FileItem file:task.getFileList()){
                if (file.isRealInputFile(task.getFileList())){
                    List <String> storage = ReplicaCatalog.getStorageList(file.getName());
                    if(storage.contains(Integer.toString(vm.getId()))){
                        filteredVms.add(vm);
                        break;
                    }
                }
            }
        }
        return filteredVms;
    }
    private boolean isContainerAffordable(Task task, Container container){
        CondorVM castedVm = (CondorVM) container.getVm();
        double relativeCostRate = castedVm.getCost() * ((double)container.getNumberOfPes() / castedVm.getNumberOfPes());
        double executionTime = (task.getTaskTotalLength() / (Parameters.VM_MIPS[0] * container.getNumberOfPes()) )+ task.getTransferTime(Parameters.VM_BW);
        double estimateTaskCost = relativeCostRate * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
        return (estimateTaskCost < task.getSubBudget());
    }

    public List<Container> getContainersWithInputData( List<Container> containerList, Task task){
        List<Container> filteredContainers = new ArrayList();
        for(Container container: containerList){
            for (FileItem file:task.getFileList()){
                if (file.isRealInputFile(task.getFileList())){
                    List <String> storage = ReplicaCatalog.getStorageList(file.getName());
                    if(storage.contains(Integer.toString(container.getVm().getId()))){
                        filteredContainers.add(container);
                        break;
                    }
                }
            }
        }
        return filteredContainers;
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
