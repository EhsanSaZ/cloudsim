package org.containerWorkflowsimِDemo;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.containerWorkflowsimِDemo.clustering.ContainerBasicClustering;
import org.containerWorkflowsimِDemo.utils.ContainerClusteringParameters;
import org.containerWorkflowsimِDemo.utils.ContainerParameters;
import org.containerWorkflowsimِDemo.utils.ContainerReplicaCatalog;
import org.workflowsim.*;
import org.workflowsim.utils.Parameters;

import java.util.ArrayList;
import java.util.List;

public class ContainerClusteringEngine extends SimEntity {
    /**
     * The task list
     */
    protected List<ContainerTask> taskList;
    /**
     * The job list
     */
    protected List<ContainerJob> jobList;
    /**
     * The task submitted list.
     */
    protected List<? extends ContainerTask> taskSubmittedList;
    /**
     * The task received list.
     */
    protected List<? extends ContainerTask> taskReceivedList;
    /**
     * The number of tasks submitted.
     */
    protected int cloudletsSubmitted;
    /**
     * The clustering engine to use
     */
    protected ContainerBasicClustering engine;
    /**
     * The WorkflowEngineId of the WorkflowEngine
     */
    private final int workflowEngineId;
    /**
     * The WorkflowEngine used in this ClusteringEngine
     */

    /**
     * The WorkflowEngine used in this ClusteringEngine
     */
    private final ContainerWorkflowEngine workflowEngine;

    public ContainerClusteringEngine(String name, int schedulers) throws Exception {
        super(name);
        setJobList(new ArrayList<>());
        setTaskList(new ArrayList<>());
        setTaskSubmittedList(new ArrayList<>());
        setTaskReceivedList(new ArrayList<>());

        cloudletsSubmitted = 0;
        this.workflowEngine = new ContainerWorkflowEngine(name + "_Engine_0", schedulers);
        this.workflowEngineId = this.workflowEngine.getId();
    }

    public int getWorkflowEngineId() {
        return this.workflowEngineId;
    }

    public ContainerWorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }

    public void submitTaskList(List<ContainerTask> list) {
        getTaskList().addAll(list);
    }

    protected void processClustering() {
        ContainerClusteringParameters params = ContainerParameters.getClusteringParameters();
        switch (params.getClusteringMethod()) {
//            /**
//             * Perform Horizontal Clustering
//             */
//            case HORIZONTAL:
//                // if clusters.num is set in configuration file
//                if (params.getClustersNum() != 0) {
//                    this.engine = new HorizontalClustering(params.getClustersNum(), 0);
//                } // if clusters.size is set in configuration file
//                else if (params.getClustersSize() != 0) {
//                    this.engine = new HorizontalClustering(0, params.getClustersSize());
//                }
//                break;
//            /**
//             * Perform Vertical Clustering
//             */
//            case VERTICAL:
//                int depth = 1;
//                this.engine = new VerticalClustering(depth);
//                break;
//            /**
//             * Perform Block Clustering
//             */
//            case BLOCK:
//                this.engine = new BlockClustering(params.getClustersNum(), params.getClustersSize());
//                break;
//            /**
//             * Perform Balanced Clustering
//             */
//            case BALANCED:
//                this.engine = new BalancedClustering(params.getClustersNum());
//                break;
            /**
             * By default, it does no clustering
             */
            default:
                this.engine = new ContainerBasicClustering();
                break;
        }
        engine.setTaskList(getTaskList());
        engine.run();
        setJobList(engine.getJobList());
    }

    protected void processDatastaging() {
        List<ContainerFileItem> list = this.engine.getTaskFiles();

        /**
         * A bug of cloudsim, you cannot set the length of a cloudlet to be
         * smaller than 110 otherwise it will fail The reason why we set the id
         * of this job to be getJobList().size() is so that the job id is the
         * next available id
         */
        // TODO EHSAN set memory
        ContainerJob job = new ContainerJob(getJobList().size(), 110, 0);

        /**
         * This is a very simple implementation of stage-in job, in which we Add
         * all the files to be the input of this stage-in job so that
         * WorkflowSim will transfers them when this job is executed
         */
        List<ContainerFileItem> fileList = new ArrayList<>();
        for (ContainerFileItem file : list) {
            /**
             * To avoid duplicate files
             */
            if (file.isRealInputFile(list)) {
                ContainerReplicaCatalog.addFileToStorage(file.getName(), Parameters.SOURCE);
                fileList.add(file);
            }
        }
        job.setFileList(fileList);
        job.setClassType(ContainerParameters.ClassType.STAGE_IN.value);

        /**
         * stage-in is always first level job
         */
        job.setDepth(0);
        job.setPriority(0);

        /**
         * A very simple strategy if you have multiple schedulers and
         * sub-workflows just use the first scheduler
         */
        job.setUserId(getWorkflowEngine().getSchedulerId(0));

        /**
         * add stage-in job
         */
        for (ContainerJob cJob : getJobList()) {
            /**
             * first level jobs
             */
            if (cJob.getParentList().isEmpty()) {
                cJob.addParent(job);
                job.addChild(cJob);
            }
        }
        getJobList().add(job);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case WorkflowSimTags.START_SIMULATION:
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                List list = (List) ev.getData();
                setTaskList(list);
                /**
                 * It doesn't mean we must do clustering here because by default
                 * the processClustering() does nothing unless in the
                 * configuration file we have specified to use clustering
                 */
                processClustering();
                /**
                 * Add stage-in jobs Currently we just add a job that has
                 * minimum runtime but inputs all input data at the beginning of
                 * the workflow execution
                 */
                processDatastaging();
                sendNow(this.workflowEngineId, WorkflowSimTags.JOB_SUBMIT, getJobList());
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
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
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        schedule(getId(), 0, WorkflowSimTags.START_SIMULATION);
    }

    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }


    protected void setTaskList(List<ContainerTask> taskList) {
        this.taskList = taskList;
    }

    public List<ContainerTask> getTaskList() {
        return (List<ContainerTask>) taskList;
    }

    protected void setJobList(List<ContainerJob> jobList) {
        this.jobList = jobList;
    }

    public List<ContainerJob> getJobList() {
        return jobList;
    }

    protected void setTaskSubmittedList(List<ContainerTask> taskSubmittedList) {
        this.taskSubmittedList = taskSubmittedList;
    }

    public List<ContainerTask> getTaskSubmittedList() {
        return (List<ContainerTask>) taskSubmittedList;
    }

    protected void setTaskReceivedList(List<ContainerTask> taskReceivedList) {
        this.taskReceivedList = taskReceivedList;
    }

    public List<ContainerTask> getTaskReceivedList() {
        return (List<ContainerTask>) taskReceivedList;
    }


}
