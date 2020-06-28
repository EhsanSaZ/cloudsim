package org.containerWorkflowsimِDemo;



import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.containerWorkflowsimِDemo.planning.ContainerBasePlanningAlgorithm;
import org.containerWorkflowsimِDemo.planning.ContainerRandomPlanningAlgorithm;
import org.containerWorkflowsimِDemo.utils.ContainerParameters;
import org.workflowsim.WorkflowSimTags;

import java.util.ArrayList;
import java.util.List;

public class ContainerWorkflowPlanner extends SimEntity  {
    /**
     * The task list.
     */
    protected List<ContainerTask> taskList;
    /**
     * The workflow parser.
     */
    protected ContainerWorkflowParser parser;
    /**
     * The associated clustering engine.
     */
    private int clusteringEngineId;
    private ContainerClusteringEngine clusteringEngine;

    public ContainerWorkflowPlanner(String name) throws Exception {
        this(name, 1);
    }

    public ContainerWorkflowPlanner(String name, int schedulers) throws Exception {
        super(name);

        setTaskList(new ArrayList<>());
        this.clusteringEngine = new ContainerClusteringEngine(name + "_Merger_", schedulers);
        this.clusteringEngineId = this.clusteringEngine.getId();
        this.parser = new ContainerWorkflowParser(getClusteringEngine().getWorkflowEngine().getSchedulerId(0));

    }
    public int getClusteringEngineId() {
        return this.clusteringEngineId;
    }

    public ContainerWorkflowParser getWorkflowParser() {
        return this.parser;
    }

    public int getWorkflowEngineId() {
        return getClusteringEngine().getWorkflowEngineId();
    }

    public ContainerWorkflowEngine getWorkflowEngine() {
        return getClusteringEngine().getWorkflowEngine();
    }

    public ContainerClusteringEngine getClusteringEngine() {
        return this.clusteringEngine;
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case WorkflowSimTags.START_SIMULATION:
                getWorkflowParser().parse();
                setTaskList(getWorkflowParser().getTaskList());
                processPlanning();
                processImpactFactors(getTaskList());
                sendNow(getClusteringEngineId(), WorkflowSimTags.JOB_SUBMIT, getTaskList());
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void processPlanning() {
        if (ContainerParameters.getPlanningAlgorithm().equals(ContainerParameters.PlanningAlgorithm.INVALID)) {
            return;
        }
        ContainerBasePlanningAlgorithm planner = getPlanningAlgorithm(ContainerParameters.getPlanningAlgorithm());

        planner.setTaskList(getTaskList());
        planner.setVmList(getWorkflowEngine().getAllVmList());
        List list = new ArrayList();
        list = getWorkflowEngine().getAllContainerList();

        planner.setContainerList(getWorkflowEngine().getAllContainerList());
        try {
            planner.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }
    }

    private ContainerBasePlanningAlgorithm getPlanningAlgorithm(ContainerParameters.PlanningAlgorithm name) {
        ContainerBasePlanningAlgorithm planner;

        // choose which scheduler to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is FCFS_SCH
            case INVALID:
                planner = null;
                break;
            case RANDOM:
                planner = new ContainerRandomPlanningAlgorithm();
                break;
//            case HEFT:
//                planner = new HEFTPlanningAlgorithm();
//                break;
//            case DHEFT:
//                planner = new DHEFTPlanningAlgorithm();
//                break;
            default:
                planner = null;
                break;
        }
        return planner;
    }

    private void processImpactFactors(List<ContainerTask> taskList) {
        List<ContainerTask> exits = new ArrayList<>();
        for (ContainerTask task : taskList) {
            if (task.getChildList().isEmpty()) {
                exits.add(task);
            }
        }
        double avg = 1.0 / exits.size();
        for (ContainerTask task : exits) {
            addImpact(task, avg);
        }
    }

    /**
     * Add impact factor for one particular task
     *
     * @param task, the task
     * @param impact , the impact factor
     */
    private void addImpact(ContainerTask task, double impact) {

        task.setImpact(task.getImpact() + impact);
        int size = task.getParentList().size();
        if (size > 0) {
            double avg = impact / size;
            for (ContainerTask parent : task.getParentList()) {
                addImpact(parent, avg);
            }
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

    protected void finishExecution() {
        //sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
    }

    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printLine("Starting WorkflowSim " + ContainerParameters.getVersion());
        Log.printLine(getName() + " is starting...");
        schedule(getId(), 0, WorkflowSimTags.START_SIMULATION);
    }
    @SuppressWarnings("unchecked")
    public List<ContainerTask> getTaskList() {
        return (List<ContainerTask>) taskList;
    }
    protected void setTaskList(List<ContainerTask> taskList) {
        this.taskList = taskList;
    }

}
