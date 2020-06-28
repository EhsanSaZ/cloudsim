package org.containerWorkflowsimDemo;

public class ContainerWorkflowSimTags {
    /**
     * Starting constant value for cloud-related tags *
     */
    private static final int BASE = 1020;
//    /**
//     * VM Status is ready (not used)
//     */
//    public static final int VM_STATUS_READY = BASE + 2;
//    /**
//     * VM Status is busy (no jobs should run on this vm)
//     */
//    public static final int VM_STATUS_BUSY = BASE + 3;
//    /**
//     * VM Status is idle (a job can run on this vm)
//     */
//    public static final int VM_STATUS_IDLE = BASE + 4;
//    public static final int START_SIMULATION = BASE + 0;
//    public static final int JOB_SUBMIT = BASE + 1;
//    public static final int CLOUDLET_UPDATE = BASE + 5;
    public static final int CLOUDLET_CHECK = BASE + 6;

    /**
     * Private Constructor
     */
    private ContainerWorkflowSimTags() {
        throw new UnsupportedOperationException("WorkflowSim Tags cannot be instantiated");
    }
}
