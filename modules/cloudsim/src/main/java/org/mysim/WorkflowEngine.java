package org.mysim;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.mysim.budgetdistribution.BudgetDistributionStrategy;
import org.mysim.deadlinedistribution.DeadlineDistributionStrategy;
import org.mysim.planning.MyPlanningAlgorithm;
import org.mysim.planning.PlanningAlgorithmStrategy;
import org.mysim.utils.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WorkflowEngine extends SimEntity {

    private WorkflowParser workflowParser;
    private WorkflowDatacenterBroker broker;

    private DeadlineDistributionStrategy deadlineDistributor;
    private BudgetDistributionStrategy budgetDistributor;

    private PlanningAlgorithmStrategy planner;

    private QOSGenerator qosGenerator;

    private List<Workflow> workflowList;
    private List<Workflow> executedWorkflowList;

    protected List<Task> readyTaskList;
    protected List<Task> scheduledTaskList;

    protected List<? extends Container> newRequiredContainers;
    protected List<? extends Container> submittedNewRequiredContainers;

    protected List<? extends ContainerVm> newRequiredVms;
    protected List<? extends ContainerVm> submittedNewRequiredVms;

    protected List<? extends Container> newRequiredContainersOnNewVms;
    protected List<? extends Container> submittedNewRequiredContainersOnNewVms;

    private final PoissonDistribution poissonDistribution;

    private boolean isRunning= true;

    public WorkflowEngine(String name) {
        super(name);

        setQosGenerator(new QOSGenerator());

        setWorkflowList(new ArrayList<>());
        setExecutedWorkflowList(new ArrayList<>());
        setReadyTaskList(new ArrayList<>());
        setScheduledTaskList(new ArrayList<>());

        setNewRequiredContainers(new ArrayList<>());
        setSubmittedNewRequiredContainers(new ArrayList<>());

        setNewRequiredVms(new ArrayList<>());
        setSubmittedNewRequiredVms(new ArrayList<>());

        setNewRequiredContainersOnNewVms(new ArrayList<>());
        setSubmittedNewRequiredContainersOnNewVms(new ArrayList<>());

        poissonDistribution = new PoissonDistribution( 60/Parameters.ARRIVAL_RATE);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case MySimTags.SUBMIT_NEXT_WORKFLOW:
                processNextWorkflowSubmit(ev);
                break;
            case MySimTags.DO_MONITORING:
                processMonitoringVms(ev);
                break;
            case MySimTags.SCHEDULING_READY_TQ:
                processPlanningReadyTaskList();
                break;
            case MySimTags.CHECK_FINISHED_STATUS:
                processFinishedStatus(ev);
                break;
            case CloudSimTags.VM_CREATE_ACK:
                // on vm create ack we should check all the containers they must deploy on this vm
                // and submit them through broker
                processVmCreate(ev);
                break;
            case containerCloudSimTags.CONTAINER_CREATE_ACK:
                // on container create ack we should check all the task they must deploy on this container
                // and submit them through broker in datacenter
                processContainerCreate(ev);
                break;
//            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
//                // on task submit ack we should log this... and any thing needed
//                processCloudletSubmitAck(ev);
//                break;
            case CloudSimTags.CLOUDLET_RETURN:
                // on task return ack we should log and collect all ready tasks for scheduling...
                //
                processTaskReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    public void processNextWorkflowSubmit(SimEvent ev) {
        // get and parse a new workflow
        // collect all ready task
        Workflow wf = workflowParser.get_next_workflow();
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": process new workflow for submission");
        processDatastaging(wf);

        wf.setTaskNumbers(wf.getTaskList().size());

        // T ODO EHSAN: generate Qos for workflow
        getQosGenerator().setWorkflow(wf);
        getQosGenerator().run();// finish is called in run
//        getQosGenerator().finish();

        // add budget dist for workflow
        deadlineDistributor.setWorkflow(wf);
        deadlineDistributor.run();

        budgetDistributor.calculateSubBudgetWholeWorkflow(wf);

        getWorkflowList().add(wf);
        collectReadyTaskList();

        if (workflowParser.hasNextWorkflow()) {
            // T ODO EHSAN: use a delay with a poisson distribution
            schedule(this.getId(), poissonDistribution.sample(), MySimTags.SUBMIT_NEXT_WORKFLOW, null);
        }else {
            schedule(this.getId(), Parameters.CHECK_FINISHED_STATUS_DELAY, MySimTags.CHECK_FINISHED_STATUS, null);
        }

    }

    public void processMonitoringVms(SimEvent ev){
        if (false){
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), " Start monitoring and search for Idle Vms.");
            List <ContainerVm> vmToDestroyList = new ArrayList<>();
//            for (Workflow w: getWorkflowList()){
//                Log.printConcatLine("Workflow #", w.getWorkflowId(), " with ", w.getTaskList().size(), "tasks and ",
//                        w.getSubmittedTaskList().size(), " submitted tasks is remaining");
//            }
            // T ODO EHSAN: calculate the list according to vm state history from broker crated vm list..
            for (ContainerVm vm : broker.getVmsCreatedList()){
                CondorVM castedVm = (CondorVM) vm;
                if(castedVm.getAvailablePeNumbersForSchedule()==castedVm.getPeList().size()){
                    List<VmStateEntry> busyStateHistory =  castedVm.getBusyStateHistory();
                    double oldestIdleTime = Double.MAX_VALUE;
                    for (VmStateEntry stateEntry: busyStateHistory){
                        if (stateEntry.getState() == MySimTags.VM_STATUS_BUSY){
                            break;
                        }
                        oldestIdleTime = stateEntry.getTime();
                    }
                    if ((CloudSim.clock() - oldestIdleTime) > Parameters.VM_THRESH_HOLD_FOR_SHUTDOWN){
                        vmToDestroyList.add(vm);
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Select Vm #", vm.getId() , " to destroy");
                    }
                }
            }
            // T ODO EHSAN: remove this vm as a storage in replica
            for(ContainerVm vm: vmToDestroyList){
                ReplicaCatalog.removeStorageFromStorageList(Integer.toString(vm.getId()));
            }
            broker.destroyVms(vmToDestroyList);

            schedule(this.getId(), Parameters.MONITORING_INTERVAL, MySimTags.DO_MONITORING,null);
        }
    }

    public void processDatastaging(Workflow wf){
        if (wf.getTaskList() != null && !wf.getTaskList().isEmpty() ){
            List<FileItem> allFileList = new ArrayList<>();
            for (Task task : wf.getTaskList()){
                for (FileItem file : task.getFileList()){
                    if (file.getType() == Parameters.FileType.INPUT && !allFileList.contains(file)) {
                        allFileList.add(file);

                    }else if (file.getType() == Parameters.FileType.OUTPUT){
                        allFileList.add(file);
                    }
                }
            }
            /**
             * A bug of cloudsim, you cannot set the length of a cloudlet to be
             * smaller than 110 otherwise it will fail The reason why we set the id
             * of this job to be workflowParser.getJobIdStartsFrom() is so that the job id is the
             * next available id
             */
            Task stageInTask = new Task(workflowParser.getJobIdStartsFrom(),wf.getWorkflowId(),110,1);
            workflowParser.setJobIdStartsFrom(workflowParser.getJobIdStartsFrom() + 1);

            List<FileItem> fileList = new ArrayList<>();
            double fileSize = 0.0;
            for (FileItem file : allFileList){
                if (file.isRealInputFile(allFileList)){
                    ReplicaCatalog.addFileToStorage(file.getName(), Parameters.SOURCE);
                    fileList.add(file);
                    fileSize += file.getSize();
                }
            }

            fileSize /= fileList.size();
            stageInTask.setFileList(fileList);
            stageInTask.setMemory(Math.ceil(fileSize / Consts.MILLION));
            stageInTask.setClassType(Parameters.ClassType.STAGE_IN.value);

            stageInTask.setDepth(0);
            stageInTask.setPriority(0);
            stageInTask.setUserId(workflowParser.getUserId());

            for (Task task : wf.getTaskList()){
                if (task.getParentList().isEmpty()){
                    task.addParent(stageInTask);
                    stageInTask.addChild(task);
                }
            }
            wf.getTaskList().add(stageInTask);
        }
    }
    public void processFinishedStatus(SimEvent ev){
//        Log.printConcatLine(CloudSim.clock(), ": ", getName(), "Check finished status");
//        boolean flag = true;
//        // T ODO checl finish status with total number of executed workflows and initial number of workflows in parser
//        for (Workflow w : getWorkflowList()){
//            if(w.getExecutedTaskList().size() != w.getTaskNumbers()){// T ODO FIX: condition is not right
//                flag = false;
//                break;
//            }
//        }
        if (getWorkflowParser().getTotalWorkflowNumbers() == getExecutedWorkflowList().size()){
//            Log.printConcatLine(CloudSim.clock(), ": ", getName(), " All workflows are executed completely");
            this.isRunning = false;
            schedule(this.getId(), 0,CloudSimTags.END_OF_SIMULATION, null );
            // T ODO EHSAN: do any extra needed action here..like signal to other entities...
        }else {
//            Log.printConcatLine(CloudSim.clock(), ": ", getName(), " Simulation is not finished yet");
            // T ODO EHSAN: use an appropriate delay
            schedule(this.getId(), Parameters.CHECK_FINISHED_STATUS_DELAY, MySimTags.CHECK_FINISHED_STATUS, null);
        }
    }
    public void processVmCreate(SimEvent ev) {
        // submit all new containers on new vms...
//        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Vm creation Ack received");
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            ContainerVm vm = ContainerVmList.getById(getSubmittedNewRequiredVms(), vmId);
            if (vm != null){
                getSubmittedNewRequiredVms().remove(vm);
                List list = new ArrayList<>();
                for( Container c : getNewRequiredContainersOnNewVms()){
                    if(c.getVm().getId() == vmId){
                        list.add(c);
                    }
                }
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Submitting scheduled containers on Vm #", vm.getId());
                broker.submitContainerListDynamic(list);
                getNewRequiredContainersOnNewVms().removeAll( list);
                getSubmittedNewRequiredContainers().addAll(list);
            }

        }else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }
    }

    public void processContainerCreate(SimEvent ev) {
//        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Container creation Ack received");
        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        int containerId = data[1];
        int result = data[2];
        if (result == CloudSimTags.TRUE) {
            if(vmId ==-1){
                Log.printConcatLine("Error : Where is the VM");
            }else {
                // T ODO TEST : FIX LOGIC HERE
                Container c = ContainerList.getById(getSubmittedNewRequiredContainers(), containerId);
                if (c != null){
                    getSubmittedNewRequiredContainers().remove(c);
                    submitTasksOnContainer(containerId, vmId);
                    return;
                }else{
                    c = ContainerList.getById(getSubmittedNewRequiredContainersOnNewVms(), containerId);
                    if (c != null){
                        getSubmittedNewRequiredContainersOnNewVms().remove(c);
                        submitTasksOnContainer(containerId, vmId);
                        return;
                    }
                }
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": No Container with Id",containerId ," exists in submitted container list");
            }
        }else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }
    }

    public void submitTasksOnContainer(int containerId, int vmId){
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Submitting scheduled tasks on Vm #", vmId,
                " and Container #", containerId);

        List <Task> list = new ArrayList<>();
        for(Task t: getScheduledTaskList()){
            if(t.getContainerId() == containerId && t.getVmId() == vmId){
                list.add(t);
//                getReadyTaskList().remove(t);
                Workflow w = WorkflowList.getById(getWorkflowList(), t.getWorkflowID());
                assert w != null;
                w.getSubmittedTaskList().add(t);
            }
        }
        getScheduledTaskList().removeAll(list);
        broker.submitTaskListDynamic(list);
        list.clear();
    }
    public void processCloudletSubmitAck(SimEvent ev) {
        // not implemented yet
    }

    public void processTaskReturn(SimEvent ev) {
        // set this task on executed task list for related workflow
        //update budget and deadline for remaining task...

        // collect all ready task ???
        // collect all ready among the child of this returened task...??

        ContainerCloudlet cloudlet = (ContainerCloudlet) ev.getData();
        Task task = (Task) cloudlet;
        task.setTaskExecutionCost(task.getProcessingCost());
        task.setTaskExecutionTime(task.getActualCPUTime());
        Workflow w = WorkflowList.getById(getWorkflowList(), task.getWorkflowID());
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Task #", task.getCloudletId(), " is Returned");
        if (w != null) {
            w.getSubmittedTaskList().remove(task);
            w.getExecutedTaskList().add(task);
            w.setTotalCost(w.getTotalCost() + task.getTaskExecutionCost());
            if (w.getTaskList().size() > 0){
                deadlineDistributor.setWorkflow(w);
                deadlineDistributor.updateSubDeadlines();

                budgetDistributor.calculateSubBudgetWholeWorkflow(w);

//                collectReadyTaskList(task, w);
                collectReadyTaskList(w);
            } else if (w.getExecutedTaskList().size() == w.getTaskNumbers()) { //T ODO FIX: condtion is bug
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Execution of workflow ", w.getName() , " workflow #", w.getWorkflowId(), " is finished.",
                        " Deleting related files in replica catalog, clear task list.");
                // execution of this workflow is finished..
                // delete all files from replica catalog
                List<FileItem> allFileList = new ArrayList<>();
                for (Task eTask : w.getExecutedTaskList()){
                    for (FileItem file : eTask.getFileList()){
                        ReplicaCatalog.deleteFile(file.getName());
                        ReplicaCatalog.removeFileStorage(file.getName());
//                        if (file.getType() == Parameters.FileType.INPUT && !allFileList.contains(file)) {
//                            allFileList.add(file);
//
//                        }else if (file.getType() == Parameters.FileType.OUTPUT){
//                            allFileList.add(file);
//                        }
                    }
                }
//                for (FileItem file: allFileList){
////                    if (ReplicaCatalog.containsFile(file.getName())){
//                        ReplicaCatalog.deleteFile(file.getName());
//                        ReplicaCatalog.removeFileStorage(file.getName());
////                    }
//                }
                // T ODO 1- if wf is finished  calculate last make span and update final makeSpan...
                //  2- set task list to new Array list
                //  3- remove wf from workflow list add it to executed workflows list
                w.setFinalMakeSpan(w.getCurrentMakeSpan());
//                for (Task eTask: w.getExecutedTaskList()){
//                    eTask = null;
//                }
                w.setExecutedTaskList(new ArrayList<>());
                getWorkflowList().remove(w);
                getExecutedWorkflowList().add(w);
            }

        }
    }

    public void collectReadyTaskList() {
        // get all ready tasks among all workflows.. tasks with all parents executed
        // start planning them
//        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Collecting all ready tasks from all workflows");
        for (Workflow w : getWorkflowList()) {
            collectReadyTaskList(w);
        }

//        processPlanningReadyTaskList();
    }

    private boolean hasTaskListContainsID(List taskList, int id) {
        for (Iterator it = taskList.iterator(); it.hasNext(); ) {
            Task task = (Task) it.next();
            if (task.getCloudletId() == id) {
                return true;
            }
        }
        return false;
    }

    public void collectReadyTaskList(Workflow w) {
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Updating ready task queue. ",
                "collecting tasks from workflow #", w.getWorkflowId());
        List<Task> list = w.getTaskList();
        int num = list.size();
        for (int i = 0; i < num; i++) {
            Task task = list.get(i);
            //Dont use job.isFinished() it is not right
            if (!hasTaskListContainsID(w.getExecutedTaskList(), task.getCloudletId())) {
                List<Task> parentList = task.getParentList();
                boolean flag = true;
                for (Task p : parentList) {
                    if (!hasTaskListContainsID(w.getExecutedTaskList(), p.getCloudletId())) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    getReadyTaskList().add(task);
                    // ready task should be removed now. removing  later on return  may cause to multiple submissions
                    list.remove(task);
                    i--;
                    num--;
                }
            }
        }
//        processPlanningReadyTaskList();
    }

    public void processPlanningReadyTaskList() {
        if (isRunning){
//            Log.printConcatLine(getReadyTaskList().size());
            if ( getReadyTaskList().size() > 0){
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Start planning Ready Task Queue");
                MyPlanningAlgorithm planning_algorithm = (MyPlanningAlgorithm) getPlanner();
                planning_algorithm.ScheduleTasks( broker, getReadyTaskList(), getScheduledTaskList(),
                        getNewRequiredContainers(), getNewRequiredVms(), getNewRequiredContainersOnNewVms());
                // submit new containers on already running vms and submit new required vms..
                // submit tasks on running containers
                if (planning_algorithm.getScheduledTasksOnRunningContainers().size() >0){
                    for(Task task : planning_algorithm.getScheduledTasksOnRunningContainers()){
                        if (task.getVmId() != -1 && task.getContainerId()!= -1){
                            Workflow w = WorkflowList.getById(getWorkflowList(), task.getWorkflowID());
                            assert w != null;
                            w.getSubmittedTaskList().add(task);
                            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Task #", task.getCloudletId(),
                                    " will run on already running container.");

                        }
                    }
                    getScheduledTaskList().removeAll(planning_algorithm.getScheduledTasksOnRunningContainers());
                    broker.submitTaskListDynamic(planning_algorithm.getScheduledTasksOnRunningContainers());
                    planning_algorithm.clear();
                }

                // first submit new containers on already running vms--> on receive container create ack submit tasks on these containers...
                if (getNewRequiredContainers().size() > 0){
                    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Submitting new containers on already running Vms");
                    broker.submitContainerListDynamic(getNewRequiredContainers());
                    getSubmittedNewRequiredContainers().addAll(getNewRequiredContainers());
                    getNewRequiredContainers().clear();
//        setNewRequiredContainers(new ArrayList<>());
                }
                if (getNewRequiredVms().size() > 0){
                    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Submitting new Vms");
                    // second submit new vms --> on receive vm create ack submits new containers on new vms on these new vms...
                    broker.createVMsInDataCenterDynamic(getNewRequiredVms());
                    getSubmittedNewRequiredVms().addAll(getNewRequiredVms());
                    getNewRequiredVms().clear();
//        setNewRequiredVms(new ArrayList<>());
                }
            }
            schedule(this.getId(), Parameters.R_T_Q_SCHEDULING_INTERVAL, MySimTags.SCHEDULING_READY_TQ, null);
        }
    }

    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
            return;
        }
        Log.printLine(getName() + ".processOtherEvent(): "
                + "Error - event unknown by this DatacenterBroker.");
    }

    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        schedule(this.getId(),  poissonDistribution.sample(), MySimTags.SUBMIT_NEXT_WORKFLOW, null);
        schedule(this.getId(), Parameters.R_T_Q_SCHEDULING_INTERVAL, MySimTags.SCHEDULING_READY_TQ, null);
        schedule(this.getId(), Parameters.MONITORING_INTERVAL, MySimTags.DO_MONITORING,null);
    }

    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
        this.isRunning = false;
    }

    // Setter and Getter functions
    public WorkflowParser getWorkflowParser() { return workflowParser; }

    public void setWorkflowParser(WorkflowParser workflowParser) { this.workflowParser = workflowParser; }

    public DeadlineDistributionStrategy getDeadlineDistributor() { return deadlineDistributor; }

    public void setDeadlineDistributor(DeadlineDistributionStrategy deadlineDistributor) { this.deadlineDistributor = deadlineDistributor; }

    public BudgetDistributionStrategy getBudgetDistributor() { return budgetDistributor; }

    public void setBudgetDistributor(BudgetDistributionStrategy budgetDistributor) { this.budgetDistributor = budgetDistributor; }

    public PlanningAlgorithmStrategy getPlanner() { return planner; }

    public void setPlanner(PlanningAlgorithmStrategy planner) { this.planner = planner; }

    public WorkflowDatacenterBroker getBroker() { return broker; }

    public void setBroker(WorkflowDatacenterBroker broker) { this.broker = broker; }

    public List<Workflow> getWorkflowList() { return workflowList; }

    public void setWorkflowList(List<Workflow> workflowList) { this.workflowList = workflowList; }

    public List<Workflow> getExecutedWorkflowList() { return executedWorkflowList; }

    public void setExecutedWorkflowList(List<Workflow> executedWorkflowList) { this.executedWorkflowList = executedWorkflowList; }

    public List<Task> getReadyTaskList() { return readyTaskList; }

    public void setReadyTaskList(List<Task> readyTask) { this.readyTaskList = readyTask; }

    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getNewRequiredVms() {
        return (List<T>) newRequiredVms;
    }

    public <T extends ContainerVm> void setNewRequiredVms(List<T> newRequiredVms) {
        this.newRequiredVms = newRequiredVms;
    }

    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getNewRequiredContainers() {
        return (List<T>) newRequiredContainers;
    }

    public <T extends Container> void setNewRequiredContainers(List<T> newRequiredContainers) {
        this.newRequiredContainers = newRequiredContainers;
    }

    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getNewRequiredContainersOnNewVms() {
        return (List<T>) newRequiredContainersOnNewVms;
    }

    public void setNewRequiredContainersOnNewVms(List<? extends Container> newRequiredContainersOnNewVms) {
        this.newRequiredContainersOnNewVms = newRequiredContainersOnNewVms;
    }

    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getSubmittedNewRequiredContainers() {
        return (List<T>) submittedNewRequiredContainers;
    }

    public <T extends Container> void setSubmittedNewRequiredContainers(List<T> submittedNewRequiredContainers) {
        this.submittedNewRequiredContainers = submittedNewRequiredContainers;
    }

    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getSubmittedNewRequiredVms() {
        return (List<T>) submittedNewRequiredVms;
    }

    public <T extends ContainerVm> void setSubmittedNewRequiredVms(List<T> submittedNewRequiredVms) {
        this.submittedNewRequiredVms = submittedNewRequiredVms;
    }

    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getSubmittedNewRequiredContainersOnNewVms() {
        return (List<T>) submittedNewRequiredContainersOnNewVms;
    }

    public <T extends Container> void setSubmittedNewRequiredContainersOnNewVms(List<T> submittedNewRequiredContainersOnNewVms) {
        this.submittedNewRequiredContainersOnNewVms = submittedNewRequiredContainersOnNewVms;
    }

    public List<Task> getScheduledTaskList() { return scheduledTaskList; }

    public void setScheduledTaskList(List<Task> scheduledTaskList) { this.scheduledTaskList = scheduledTaskList; }

    public QOSGenerator getQosGenerator() {
        return qosGenerator;
    }

    public void setQosGenerator(QOSGenerator qosGenerator) {
        this.qosGenerator = qosGenerator;
    }

}
