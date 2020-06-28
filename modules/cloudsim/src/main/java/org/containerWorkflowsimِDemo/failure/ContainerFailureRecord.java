package org.containerWorkflowsimِDemo.failure;

public class ContainerFailureRecord {
    // TODO EHSAN failure for containers should be added
    /**
     * Length
     */
    public double length;
    /**
     * number of failed tasks.
     */
    public int failedTasksNum;
    /**
     * the depth (also used as type in some cases) of a failure.
     */
    public int depth;//used as type
    /**
     * all the tasks (failed or not).
     */
    public int allTaskNum;
    /**
     * the location.
     */
    public int vmId;
    /**
     * the job id.
     */
    public int jobId;
    /**
     * the workflow id (user id in this version).
     */
    public int workflowId;
    /**
     * delay length.
     */
    public int containerId;

    public double delayLength;

    /**
     * Initialize a Failure Record
     *
     * @param length,  length of this task
     * @param tasks,   number of failed tasks
     * @param depth,   depth of the failed tasks
     * @param all,     all the tasks
     * @param vm,      vm id where the failure generates
     * @param job,     job id where the failure generates
     * @param workflow , workflow id where the failure generates
     */
    public ContainerFailureRecord(double length, int tasks, int depth, int all, int vm, int job, int workflow, int container) {
        this.length = length;
        this.failedTasksNum = tasks;
        this.depth = depth;
        this.allTaskNum = all;
        this.vmId = vm;
        this.jobId = job;
        this.workflowId = workflow;
        this.containerId = container;
    }
}
