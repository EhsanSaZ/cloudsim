package org.mysim.utils;

public class MySimTags {
    /**
     * Starting constant value for cloud-related tags *
     */
    private static final int BASE = 1000;
    /**
     * VM Status is ready (not used)
     */
    public static final int VM_STATUS_READY = BASE + 2;
    /**
     * VM Status is busy (no jobs should run on this vm)
     */
    public static final int VM_STATUS_BUSY = BASE + 3;
    /**
     * VM Status is idle (a job can run on this vm)
     */
    public static final int VM_STATUS_IDLE = BASE + 4;

    public static final int START_SIMULATION = BASE + 0;

    public static final int JOB_SUBMIT = BASE + 1;

    public static final int CLOUDLET_UPDATE = BASE + 5;

    public static final int CLOUDLET_CHECK = BASE + 6;


    public static final int SUBMIT_NEXT_WORKFLOW = BASE + 7;
    public static final int SCHEDULING_READY_TQ = BASE + 8;
    public static final int CHECK_FINISHED_STATUS = BASE + 9;
//    public static final int SUBMIT_VM = BASE + 9;
//    public static final int SCHED_INTERVAL = BASE + 10;

//    public static final int SUBMIT_NEXT_WORKFLOW = BASE + 11;
//    public static final int SUBMIT_NEXT_WORKFLOW = BASE + 12;


    /**
     * Private Constructor
     */
    private MySimTags() {
        throw new UnsupportedOperationException("MySim Tags cannot be instantiated");
    }
}
