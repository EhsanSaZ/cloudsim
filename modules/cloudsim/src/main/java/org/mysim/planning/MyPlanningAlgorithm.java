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
import org.mysim.utils.MySimTags;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;

import java.util.*;
import java.util.Map.Entry;

public class MyPlanningAlgorithm extends PlanningAlgorithmStrategy{

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

    public MyPlanningAlgorithm(){
        setScheduledTasksOnRunningContainers(new ArrayList<>());
    }

    public void ScheduleTasks(WorkflowDatacenterBroker broker,
                              List<Task> readyTasks,
                              List<Task> scheduledTasks,
                              List<Container> newRequiredContainers,
                              List<ContainerVm> newRequiredVms,
                              List<Container> newRequiredContainersOnNewVms) {
        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Start scheduling Ready task Queue with ",readyTasks.size(), "tasks");
        readyTasks.sort(compareBySubDeadline);
        List<Task> waitQueue = new ArrayList<>();
        List<Task> toRemove = new ArrayList<>();
        List<Container> idleRunningContainerList = new ArrayList<>();

        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Collecting idle Running Containers");
        for (Container container: broker.getContainersCreatedList()){
            if(container.getWorkloadMips() == 0){
                idleRunningContainerList.add(container);
            }
        }
//        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Setting isScheduled to false for Vms with 0 containers");
//        for (ContainerVm vm: broker.getVmsCreatedList()){
//            CondorVM castedVm = (CondorVM) vm;
//            assert castedVm != null;
//            if(castedVm.getContainerList().size() == 0){
//                castedVm.setScheduled(false);
//            }
//        }

        // Try to schedule tasks in ready Queue on already running resources (by broker)
        for (Task task: readyTasks){
            int requiredPesNumber = calculateRequiredPesNumber(task);
            task.setNumberOfPes(requiredPesNumber);
            int requiredMemory = (int)Math.ceil(task.getMemory());

            ContainerVm provisionedVm = null;
            // Get all Vms that has at list t'(Core num, mem demand) available resources
            List <ContainerVm> AllVm= new ArrayList<>();
            for (ContainerVm vm:broker.getVmsCreatedList()){
                CondorVM castedVm = (CondorVM) vm;
                assert castedVm != null;
//                if (castedVm.getAvailablePeNumbersForSchedule() >= requiredPesNumber && castedVm.getAvailableRamForSchedule() >= requiredMemory){
//                    AllVm.add(vm);
//                }
                // T ODO EHSAN FIX: CHECK FOR STORAGE TOO
                if (castedVm.isSuitableForTask(requiredPesNumber, requiredMemory) && castedVm.getAvailableSizeForSchedule() >= Parameters.CONTAINER_SIZE){
                    AllVm.add(vm);
                }
            }
            if (AllVm.size() > 0){
                //  All_VMs  in to two  affordable and non-affordable sets
                List <ContainerVm> affordableVms= new ArrayList<>();
                List <ContainerVm> non_affordableVms= new ArrayList<>();
                for (ContainerVm vm: AllVm){
                    if (task.isVmAffordable(vm)){
                        affordableVms.add(vm);
                    }else{
                        non_affordableVms.add(vm);
                    }
                }

                if (affordableVms.size() > 0){
                    // Categorize affordable vms in to with data and no data sets
//                    List <ContainerVm> affordableVms_withInputData = new ArrayList<>();
//                    affordableVms_withInputData.addAll(getVmsWithInputData(affordableVms, task));
                    List<ContainerVm> affordableVms_withInputData = new ArrayList<>(getVmsWithInputData(affordableVms, task));
                    List<ContainerVm> affordableVms_NoInputData = new ArrayList<>(affordableVms);
                    affordableVms_NoInputData.removeAll(affordableVms_withInputData);

                    if (affordableVms_withInputData.size() >0){
                        // sort affordable vms with input data base on Bi factor
                        List<BiFactorRank> vmRankedList = sortOnFactorForTask(affordableVms_withInputData, task, Parameters.BI_FACTOR);
                        provisionedVm =  vmRankedList.get(0).vm;
                    }

                    // when it is not possible to run on a new container on affordable vm with input data
                    // look for a container with 0 workload
                    // choose one of them that is appropriate to use for running task
                    if (provisionedVm == null){
                        int index = scheduleOnRunningContainers(idleRunningContainerList, task, requiredPesNumber, requiredMemory);
                        if (index != -1){
                            scheduledTasks.add(task);
                            toRemove.add(task);
                            idleRunningContainerList.remove(index);
                            continue;
                        }
                    }
                    // when it is <<not>> possible to run the task on a idle running container
                    // deploy a new container on any remaining affordable vms
                    if (provisionedVm == null){
                        List<BiFactorRank> vmNoInputDataRankedList = sortOnFactorForTask(affordableVms_NoInputData, task,Parameters.BI_FACTOR);
                        provisionedVm =  vmNoInputDataRankedList.get(0).vm;
                    }
                }else{
                    // when it is not possible to run on a new container on non-affordable vm with input data
                    // look for a container with 0 workload
                    // choose one of them that is appropriate to use for running task
                    int index = scheduleOnRunningContainers(idleRunningContainerList, task, requiredPesNumber, requiredMemory);
                    if (index != -1){
                        scheduledTasks.add(task);
                        toRemove.add(task);
                        idleRunningContainerList.remove(index);
                        continue;
                    }

                    // try to schedule on non-affordable vms
                    // Categorize non-affordable vms in to with data and no data sets
                    List <ContainerVm> non_affordableVms_withInputData = new ArrayList<>(getVmsWithInputData(non_affordableVms, task));
                    List<ContainerVm> non_affordableVms_NoInputData = new ArrayList<>(non_affordableVms);
                    non_affordableVms_NoInputData.removeAll(non_affordableVms_withInputData);

                    List<BiFactorRank> vmRankedList;
                    if (non_affordableVms_withInputData.size() >0){
                        vmRankedList = sortOnFactorForTask(non_affordableVms_withInputData, task, Parameters.C_FACTOR);
                    }else {
                        // when it is <<not>> possible to run the task on a idle running container or create a new container on a vm with input data
                        // deploy a new container on any remaining non-affordable vms
                        vmRankedList = sortOnFactorForTask(non_affordableVms_NoInputData, task, Parameters.C_FACTOR);
                    }
                    provisionedVm =  vmRankedList.get(0).vm;
                }
                if (provisionedVm != null){
                    // T ODO EHSAN: it may have some bugs in casting.. need check
                    // 1- update scheduled capacity of vm--
                    // 2- create a new container to deploy on this vm--
                    // 3- add container to newRequiredContainers--
                    // 4- add task to scheduled task--
                    // 5- and add to remove list inorder to remove from ready tasks at the end...--
                    CondorVM vm = (CondorVM) provisionedVm;
                    vm.setAvailablePeNumbersForSchedule(vm.getAvailablePeNumbersForSchedule() - requiredPesNumber);
                    vm.setAvailableRamForSchedule(vm.getAvailableRamForSchedule() - requiredMemory);
                    vm.setAvailableSizeForSchedule(vm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);
//                    vm.setScheduled(true);
                    Container newContainer = new Container(IDs.pollId(Container.class),
                            broker.getId(),Parameters.CONTAINER_MIPS[0],
                            requiredPesNumber, requiredMemory, (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE,
                            "Xen",new ContainerCloudletSchedulerSpaceShared(),Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
                    newContainer.setVm(vm);
                    task.setVmId(vm.getId());
                    task.setContainerId(newContainer.getId());

                    Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: ",
                            "Task #", task.getCloudletId(), " scheduled to run on VM #", provisionedVm.getId(), " and Container #",
                            newContainer.getId());

                    newRequiredContainers.add(newContainer);
                    scheduledTasks.add(task);
                    toRemove.add(task);
                }

            }else{ // AllVm.size() == 0
                // when there is no running vm with required free resources at all
                // first look for a container with 0 workload
                // choose one of them that is appropriate to use for running task
                // if it is not possible too, check if delay is possible or not
                // if delay is not possible to add task to WaitQueue to run on new resources..
                int index = scheduleOnRunningContainers(idleRunningContainerList, task, requiredPesNumber, requiredMemory);
                if (index != -1){
                    scheduledTasks.add(task);
                    toRemove.add(task);
                    idleRunningContainerList.remove(index);
                    continue;
                }
                // T ODO EHSAN: check for delay if possible ignore this task and continue
                //  else add to wait queue and continue
                // check for possibility of running task on any other container on total brokers vms..
                // TODO EHSAN FIX: choose a right place for delaying task
                double remainingTimeToDeadline = task.getSubDeadline() - (CloudSim.clock() + Parameters.R_T_Q_SCHEDULING_INTERVAL);
                double minExecutionTime = task.getCloudletLength() / ( Parameters.VM_MIPS[0] * Parameters.VM_PES[Parameters.VM_TYPES_NUMBERS-1] );
                double maxExecutionTime = task.getCloudletLength() / ( Parameters.VM_MIPS[0] * Parameters.VM_PES[0] );
                if ((maxExecutionTime + task.getTransferTime(Parameters.VM_BW) > remainingTimeToDeadline)) {
                    // it is not possible to delay the task so put it on wait queue
                    // if the condition is false it means that delay is possible
                    // so do nothing and let task remains in ready task queue for next interval
                    waitQueue.add(task);
                }else{
//                    System.out.println("task is delayed");
                    Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Task# ", task.getCloudletId() , " is delayed");
                    task.setSubDeadline(remainingTimeToDeadline);
                }
//                waitQueue.add(task);
//                continue;
            }
        }
        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: Sending signal to destroy idle containers");
        // collect and destroy all containers with workload 0
        for (Container container:idleRunningContainerList){
            broker.destroyContainer(container);
        }
        idleRunningContainerList.clear();
//        readyTasks.removeAll(toRemove);
        // -------------------------------------------------- start packing tasks in Wait Queue --------------------------------------------------

        //cluster waiting tasks base on workflow and depth(level)
        Map <String, List<Task>> taskCluster = new HashMap<>();
        Map <String, Double> clustersMinDeadline = new HashMap<>();
        for (Task task: waitQueue){
            String key = task.getWorkflowID() + "-"+ task.getDepth();
            if (!taskCluster.containsKey(key)){
                taskCluster.put(key, new ArrayList<>());
                clustersMinDeadline.put(key, Double.MAX_VALUE);
            }
            taskCluster.get(key).add(task);
            if (task.getSubDeadline() < clustersMinDeadline.get(key)){
                clustersMinDeadline.put(key, task.getSubDeadline());
            }
        }

        //cluster waiting tasks base on workflow
//        Map <Integer, List<Task>> taskCluster = new HashMap<>();
//        for (Task task: waitQueue){
//            if (!taskCluster.containsKey(task.getWorkflowID())){
//                taskCluster.put(task.getWorkflowID(), new ArrayList<>());
//            }
//            taskCluster.get(task.getWorkflowID()).add(task);
//        }
//
//        for ( List<Task> tasksList: taskCluster.values()){
//                tasksList.sort(compareByDepth);
//
//        }



        // sort clustersMinDeadline map base on value deadline
        Set<Entry<String, Double>> entries = clustersMinDeadline.entrySet();
        List<Entry<String, Double>> listOfEntries = new ArrayList<Entry<String, Double>>(entries);
        listOfEntries.sort(valueComparator);

        // now tasks are grouped base on their workflow and level
        // the groups are sorted and accessed according to min deadline among each
        for(Entry<String, Double> entry : listOfEntries){
            List<Task> tasksList = taskCluster.get(entry.getKey());
            // maybe tasksList.sort(compareByDepth) is not needed
            //tasksList.sort(compareByDepth);
            for(Task task: tasksList){
                int requiredMemory = (int)Math.ceil(task.getMemory());
//                ContainerVm provisionedVm = null;
                boolean notScheduled = true;
                for (ContainerVm vm :newRequiredVms){
                    CondorVM castedVm = (CondorVM) vm;
                    assert castedVm != null;
                    // T ODO EHSAN FIX: CHECK FOR STORAGE TOO
//                    if (castedVm.getAvailablePeNumbersForSchedule() >= task.getNumberOfPes() && castedVm.getAvailableRamForSchedule() >= requiredMemory && task.isVmAffordable(vm)){
                    if (castedVm.isSuitableForTask(task.getNumberOfPes(), requiredMemory) && castedVm.getAvailableSizeForSchedule() >= Parameters.CONTAINER_SIZE && task.isVmAffordable(vm)){
                        //create a new container for running on this vm
                        castedVm.setAvailablePeNumbersForSchedule(castedVm.getAvailablePeNumbersForSchedule() - task.getNumberOfPes());
                        castedVm.setAvailableRamForSchedule(castedVm.getAvailableRamForSchedule() - requiredMemory);
                        castedVm.setAvailableSizeForSchedule(castedVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);
//                        castedVm.setState(MySimTags.VM_STATUS_BUSY);

                        Container newContainer = new Container(IDs.pollId(Container.class),
                                broker.getId(),Parameters.CONTAINER_MIPS[0],
                                task.getNumberOfPes(), requiredMemory, (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE,
                                "Xen",new ContainerCloudletSchedulerSpaceShared(),Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
                        newContainer.setVm(vm);
                        task.setVmId(vm.getId());
                        task.setContainerId(newContainer.getId());

                        Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: ",
                                "Task #", task.getCloudletId(), " scheduled to run on VM #", castedVm.getId(), " and Container #",
                                newContainer.getId());

                        newRequiredContainersOnNewVms.add(newContainer);
                        scheduledTasks.add(task);
                        toRemove.add(task);
                        notScheduled = false;
                        break;
                    }
                }
                if (notScheduled){
                    List <Integer> appropriateVmsType = new ArrayList<>();
                    for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
                        // check if type i is ok or not..
                        if (Parameters.VM_PES[i] >= task.getNumberOfPes() && Parameters.VM_RAM[i] >= requiredMemory){
                            appropriateVmsType.add(i);
                        }
                    }
                    //by default : if there is no vm type that have at least minimum number of task demand resources
                    // run task on fastest vm
                    // 1- create a new vm with max power
                    // 2- create a new container with max power
                    // schedule task
                    int VmType = Parameters.VM_TYPES_NUMBERS - 1;
                    if (appropriateVmsType.size() > 0){
                        // calculate Bi facotr for all types and deploy on the best one
//                        List<BiFactorRankVmType> vmTypesRankList = sortOnFactorForTaskVmTypes(appropriateVmsType, task, Parameters.BI_FACTOR);
//                        VmType = vmTypesRankList.get(0).vmType;
                        // radome choose 68 % success
                        VmType = rd.nextInt(appropriateVmsType.size());
                    }else{
                        task.setNumberOfPes(Math.min(task.getNumberOfPes(), Parameters.VM_PES[VmType]));
                        task.setMemory(Math.min(requiredMemory, Parameters.VM_RAM[VmType]));
                    }
                    System.out.println("chosen type "+ VmType);
                    ArrayList<ContainerPe> peList = new ArrayList<ContainerPe>();
                    for (int j = 0; j < Parameters.VM_PES[VmType]; ++j){
                        peList.add(new ContainerPe(j, new CotainerPeProvisionerSimple((double) Parameters.VM_MIPS[VmType])));
                    }
                    // T ODO EHSAN FIX: CHECK FOR STORAGE TOO
                    CondorVM newVm = new CondorVM(IDs.pollId(ContainerVm.class), broker.getId(), Parameters.VM_MIPS[VmType],
                            Parameters.VM_RAM[VmType], Parameters.VM_BW, Parameters.VM_SIZE, "Xen",
                            new ContainerSchedulerTimeSharedOverSubscription(peList),
                            new ContainerRamProvisionerSimple(Parameters.VM_RAM[VmType]),
                            new ContainerBwProvisionerSimple(Parameters.VM_BW),
                            peList, Parameters.CONTAINER_VM_SCHEDULING_INTERVAL,
                            Parameters.COST[VmType], Parameters.COST_PER_MEM[VmType],
                            Parameters.COST_PER_STORAGE[VmType], Parameters.COST_PER_BW[VmType]);

                    newVm.setAvailablePeNumbersForSchedule(newVm.getAvailablePeNumbersForSchedule() - task.getNumberOfPes());
                    newVm.setAvailableRamForSchedule(newVm.getAvailableRamForSchedule() - requiredMemory);
                    newVm.setAvailableSizeForSchedule(newVm.getAvailableSizeForSchedule() - Parameters.CONTAINER_SIZE);
//                    newVm.setState(MySimTags.VM_STATUS_BUSY);

                    Container newContainer = new Container(IDs.pollId(Container.class), broker.getId(), Parameters.CONTAINER_MIPS[0],
                            task.getNumberOfPes(), (int)Math.min(requiredMemory, Parameters.VM_RAM[VmType]),
                            (long)Parameters.CONTAINER_BW, Parameters.CONTAINER_SIZE, "Xen",
                            new ContainerCloudletSchedulerSpaceShared(),
                            Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);

                    newContainer.setVm(newVm);

                    task.setVmId(newVm.getId());
                    task.setContainerId(newContainer.getId());


                    Log.printConcatLine(CloudSim.clock(), ": PlanningAlgorithm: ",
                            "Task #", task.getCloudletId(), " scheduled to run on VM #", newVm.getId(), " and Container #",
                            newContainer.getId());

                    newRequiredVms.add(newVm);
                    newRequiredContainersOnNewVms.add(newContainer);
                    scheduledTasks.add(task);
                    toRemove.add(task);

                }
            }
        }

        // remove all scheduled tasks from ready task list
        readyTasks.removeAll(toRemove);
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

    public int scheduleOnRunningContainers( List <Container> containerList, Task task, int requiredPe, int requiredMem){
        int index = -1;
        for (Container container: containerList){
            if (container.getNumberOfPes() >= requiredPe && container.getRam() >= requiredMem){
                CondorVM castedVm =  (CondorVM) container.getVm();
                // calculate cost of execution
                double relativeCostRate = castedVm.getCost() * ( Parameters.CPU_COST_FACTOR * ((double)container.getNumberOfPes() / castedVm.getPeList().size()) +
                        ( 1 - Parameters.CPU_COST_FACTOR) * (container.getRam()/ castedVm.getRam()));
                double executionTime = (task.getCloudletLength() / (container.getNumberOfPes() * Parameters.CONTAINER_MIPS[0])) + task.getTransferTime(Parameters.VM_BW);
                double estimatedCost = relativeCostRate * Math.ceil( executionTime / Parameters.BILLING_PERIOD);

                if(estimatedCost <= task.getSubBudget()){
                    index = containerList.indexOf(container);
                    container.setWorkloadMips(Parameters.CONTAINER_MIPS[0]);
                    task.setContainerId(container.getId());
                    task.setVmId(container.getVm().getId());
//                    scheduledTasks.add(task);
//                    toremove.add(task);
                    getScheduledTasksOnRunningContainers().add(task);
                    break;
                }
            }
        }
        return index;
    }

    private class BiFactorRank implements Comparable<BiFactorRank>{
        public ContainerVm vm;
        public Double BiFactor;
        public BiFactorRank(ContainerVm vm, Double BiFactor) {
            this.vm = vm;
            this.BiFactor = BiFactor;
        }

        // descending order
        @Override
        public int compareTo( BiFactorRank o) {
            return o.BiFactor.compareTo(BiFactor);
        }
    }

    private class BiFactorRankVmType implements Comparable<BiFactorRankVmType>{
        public int vmType;
        public Double BiFactor;
        public BiFactorRankVmType(int vmType, Double BiFactor) {
            this.vmType = vmType;
            this.BiFactor = BiFactor;
        }

        // descending order
        @Override
        public int compareTo(BiFactorRankVmType o) {
            return o.BiFactor.compareTo(BiFactor);
        }
    }

    public List<BiFactorRankVmType> sortOnFactorForTaskVmTypes (List<Integer> vmTypesList, Task task, int factorType){
        List<BiFactorRankVmType> vmTypesRankList = new ArrayList<>();

        Map <Integer, Double> costMap = new HashMap<>();
        double minCost = Double.MAX_VALUE;
        for (int vmType: vmTypesList){
            double cost = task.estimateTaskCostForVmType(vmType);
            minCost = Math.min(cost, minCost);
            costMap.put(vmType, task.estimateTaskCostForVmType(vmType));
        }
        for (int vmType: vmTypesList){
            double C_factor =(task.getSubBudget() - costMap.get(vmType)) / (task.getSubBudget() - minCost);
            double U_factor = 0.0;
            if (factorType == Parameters.BI_FACTOR){
                int availablePeAfterSchedule = Parameters.VM_PES[vmType] - task.getNumberOfPes();
                double availableMemAfterSchedule = Parameters.VM_RAM[vmType] - (int)Math.ceil(task.getMemory());
                U_factor = Math.hypot( 1 - ((double)availablePeAfterSchedule / Parameters.VM_PES[vmType]), 1-(availableMemAfterSchedule / Parameters.VM_RAM[vmType] ));

//                C_factor = 0;
            }
            vmTypesRankList.add( new BiFactorRankVmType(vmType, C_factor + U_factor));
        }
        Collections.sort(vmTypesRankList);
        return vmTypesRankList;
    }

    public List<BiFactorRank> sortOnFactorForTask(List< ? extends ContainerVm> vmList, Task task, int factorType){
        List<BiFactorRank> vmRankedList = new ArrayList<>();

        Map <Integer, Double> costMap = new HashMap<>();
        double minCost = Double.MAX_VALUE;
        for (ContainerVm vm: vmList){
            double cost = task.estimateTaskCost(vm);
            minCost = Math.min(cost, minCost);
            costMap.put(vm.getId(), task.estimateTaskCost(vm));
        }

        for (ContainerVm vm: vmList){
            double C_factor = (task.getSubBudget() - costMap.get(vm.getId())) / (task.getSubBudget() - minCost);
            double U_factor = 0.0;
            if (factorType == Parameters.BI_FACTOR){
                int availablePeAfterSchedule = ((CondorVM) vm).getAvailablePeNumbersForSchedule() - task.getNumberOfPes();
                double availableMemAfterSchedule = ((CondorVM) vm).getAvailableRamForSchedule() - (int)Math.ceil(task.getMemory());
//                double U_factor = Math.sqrt(Math.pow((1 - (availablePeAfterSchedule / vm.getPeList().size())), 2) + Math.pow((1-(availableMemAfterSchedule / vm.getRam())), 2));
                U_factor = Math.hypot( 1 - ((double)availablePeAfterSchedule / vm.getPeList().size()), 1-(availableMemAfterSchedule / vm.getRam()));
            }
            vmRankedList.add( new BiFactorRank(vm, C_factor + U_factor));
        }
        // sort on descending order
        Collections.sort(vmRankedList);
        return vmRankedList;
    }

    @Override
    public void run() {

    }

    public void clear(){
        scheduledTasksOnRunningContainers.clear();
    }

    public int calculateRequiredPesNumber(Task task){
        double time = task.getSubDeadline() - task.getTransferTime(Parameters.VM_BW);
        if (time < 0){
            // just transfer time is more than deadline so use max number of cores
            return Parameters.VM_PES[Parameters.VM_TYPES_NUMBERS-1];
        }
        int peNum = (int) Math.ceil(task.getCloudletLength()  / (time * Parameters.VM_MIPS[0]));

        return Math.min(peNum, Parameters.VM_PES[Parameters.VM_TYPES_NUMBERS - 1]);
    }

    //-----------------------setter and getter
    public List<Task> getScheduledTasksOnRunningContainers() {
        return scheduledTasksOnRunningContainers;
    }

    public void setScheduledTasksOnRunningContainers(List<Task> scheduledTasksOnRunningContainers) {
        this.scheduledTasksOnRunningContainers = scheduledTasksOnRunningContainers;
    }
}
