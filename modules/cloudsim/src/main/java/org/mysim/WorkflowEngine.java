package org.mysim;

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
import org.mysim.utils.MySimTags;
import org.mysim.utils.Parameters;
import org.mysim.utils.WorkflowList;
import org.mysim.utils.ReplicaCatalog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WorkflowEngine extends SimEntity {

    private WorkflowParser workflowParser;
    private DeadlineDistributionStrategy deadlineDistributor;
    private BudgetDistributionStrategy budgetDistributor;

    private PlanningAlgorithmStrategy planner;

    private WorkflowDatacenterBroker broker;

    private List<Workflow> workflowList;
    private List<Task> readyTaskList;

    protected List<? extends Container> newRequiredContainers;
    protected List<? extends Container> submittedNewRequiredContainers;

    private List<? extends ContainerVm> newRequiredVms;
    private List<? extends ContainerVm> submittedNewRequiredVms;

    protected List<? extends Container> newRequiredContainersOnNewVms;
    protected List<? extends Container> submittedNewRequiredContainersOnNewVms;

    public WorkflowEngine(String name) {
        super(name);
        setReadyTaskList(new ArrayList<>());
        setWorkflowList(new ArrayList<>());

        setNewRequiredContainers(new ArrayList<>());
        setSubmittedNewRequiredContainers(new ArrayList<>());

        setNewRequiredVms(new ArrayList<>());
        setSubmittedNewRequiredVms(new ArrayList<>());

        setNewRequiredContainersOnNewVms(new ArrayList<>());
        setSubmittedNewRequiredContainersOnNewVms(new ArrayList<>());
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case MySimTags.SUBMIT_NEXT_WORKFLOW:
                processNextWorkflowSubmit(ev);
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
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                // on task submit ack we should log this... and any thing needed
                processCloudletSubmitAck(ev);
                break;
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
        processDatastaging(wf);

        deadlineDistributor.setWorkflow(wf);
        deadlineDistributor.run();
        getWorkflowList().add(wf);
        collectReadyTaskList();

        if (workflowParser.hasNextWorkflow()) {
            // TODO EHSAN: use a delay with a poisson distribution
            schedule(this.getId(), 500, MySimTags.SUBMIT_NEXT_WORKFLOW, null);
        }else {
            schedule(this.getId(), Parameters.CHECK_FINISHED_STATUS_DELAY, MySimTags.CHECK_FINISHED_STATUS, null);
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
            for (FileItem file : allFileList){
                if (file.isRealInputFile(allFileList)){
                    ReplicaCatalog.addFileToStorage(file.getName(), Parameters.SOURCE);
                    fileList.add(file);
                }
            }
            stageInTask.setFileList(fileList);
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
        boolean flag = true;
        for (Workflow w : getWorkflowList()){
            if(w.getTaskList().size()>0 || w.getSubmittedTaskList().size()>0){
                flag = false;
                break;
            }
        }
        if (flag){
            schedule(this.getId(), 0,CloudSimTags.END_OF_SIMULATION, null );
            // TODO EHSAN: do any extra needed action here..like signal to other entities...
        }else {
            // TODO EHSAN: use an appropriate delay
            schedule(this.getId(), Parameters.CHECK_FINISHED_STATUS_DELAY, MySimTags.CHECK_FINISHED_STATUS, null);
        }
    }
    public void processVmCreate(SimEvent ev) {
        // submit all new containers on new vms...
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
                //TODO EHSAN : new functions in broker and check for list and pointers in case of modification of this list
                //broker.submitContainerListDynamic(list);
                getNewRequiredContainersOnNewVms().removeAll( list);
                getSubmittedNewRequiredContainers().addAll(list);
            }

        }else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }
    }

    public void processContainerCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        int containerId = data[1];
        int result = data[2];
        if (result == CloudSimTags.TRUE) {
            if(vmId ==-1){
                Log.printConcatLine("Error : Where is the VM");
            }else {
                Container c = ContainerList.getById(getSubmittedNewRequiredContainers(), containerId);
                if (c != null){
                    getSubmittedNewRequiredContainers().remove(c);
                    submitTasksOnContainer(containerId, vmId);
                }else{
                    c = ContainerList.getById(getSubmittedNewRequiredContainersOnNewVms(), containerId);
                    if (c != null){
                        getSubmittedNewRequiredContainersOnNewVms().remove(c);
                        submitTasksOnContainer(containerId, vmId);
                    }
                }
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": No Container with Id",containerId ," exists in submitted container list");
            }
        }else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }
    }

    public void submitTasksOnContainer(int containerId, int vmId){
        List <Task> list = new ArrayList<>();
        for(Task t: getReadyTaskList()){
            if(t.getContainerId() == containerId && t.getVmId() == vmId){
                list.add(t);
                getReadyTaskList().remove(t);
                Workflow w = WorkflowList.getById(getWorkflowList(), t.getWorkflowID());
                assert w != null;
                w.getSubmittedTaskList().add(t);
            }
        }
        //TODO EHSAN : new functions in broker and check for list and pointers in case of modification of this list
        //broker.submitTaskListDynamic(list);
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
        Workflow w = WorkflowList.getById(getWorkflowList(), task.getWorkflowID());
        if (w != null) {
            w.getExecutedTaskList().add(task);
            w.getSubmittedTaskList().remove(task);
            if (w.getTaskList().size() > 0){
                deadlineDistributor.setWorkflow(w);
                deadlineDistributor.updateSubDeadlines();
                collectReadyTaskList(task, w);
            } else if (w.getTaskList().size() == 0 && w.getSubmittedTaskList().size() == 0) {
                // this w is done..
                // delete all files from replica catalog
                List<FileItem> allFileList = new ArrayList<>();
                for (Task t : w.getExecutedTaskList()){
                    for (FileItem file : t.getFileList()){
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
            }

        }
    }

    public void collectReadyTaskList() {
        // get all ready tasks among all workflows.. tasks with all parents executed
        // start planning them
        for (Workflow w : getWorkflowList()) {
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
                        // ready task should be removed now from workflow task list. done by pointer
                        // removing  later on return  may cause to multiple submissions
                        list.remove(task);
                        i--;
                        num--;
                    }
                }
            }
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

    public void collectReadyTaskList(Task finishedTask, Workflow w) {
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
        if ( getReadyTaskList().size() > 0){
            MyPlanningAlgorithm planning_algorithm = (MyPlanningAlgorithm) getPlanner();
            planning_algorithm.ScheduleTasks(getReadyTaskList(), broker.getVmsCreatedList(),
                    getNewRequiredContainers(), getNewRequiredVms(), getNewRequiredContainersOnNewVms());
            // submit new containers on already running vms and submit new required vms..

            // first submit new containers on already running vms--> on receive container create ack submit tasks on these containers...
            //TODO EHSAN : two new functions in broker and check for list and pointers in case of modification of this list
            if (getNewRequiredContainers().size() > 0){
                //broker.submitContainerListDynamic(getNewRequiredContainers());
                getSubmittedNewRequiredContainers().addAll(getNewRequiredContainers());
                getNewRequiredContainers().clear();
//        setNewRequiredContainers(new ArrayList<>());
            }
            if (getNewRequiredVms().size() > 0){
                // second submit new vms --> on receive vm create ack submits new containers on new vms on these new vms...
                //TODO EHSAN : two new functions in broker and check for list and pointers in case of modification of this list
                //broker.submitVmsListDynamic(getNewRequiredVms());
                getSubmittedNewRequiredVms().addAll(getNewRequiredVms());
                getNewRequiredVms().clear();
//        setNewRequiredVms(new ArrayList<>());
            }
        }
        schedule(this.getId(), Parameters.R_T_Q_SCHEDULING_INTERVAL, MySimTags.SCHEDULING_READY_TQ, null);
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
        schedule(this.getId(), 0, MySimTags.SUBMIT_NEXT_WORKFLOW, null);
        schedule(this.getId(), Parameters.R_T_Q_SCHEDULING_INTERVAL, MySimTags.SCHEDULING_READY_TQ, null);
    }

    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
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

}
