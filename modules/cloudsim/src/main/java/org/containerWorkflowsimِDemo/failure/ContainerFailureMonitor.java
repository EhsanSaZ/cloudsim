package org.containerWorkflowsimِDemo.failure;

import org.cloudbus.cloudsim.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainerFailureMonitor {
    // TODO EHSAN failure for containers should be added
    protected static Map<Integer, List<ContainerFailureRecord>> vm2record;

    protected static Map<Integer, List<ContainerFailureRecord>> type2record;

    protected static Map<Integer, ContainerFailureRecord> jobid2record;

    protected static List<ContainerFailureRecord> recordList;

    public static Map index2job;

    public static void init() {
        vm2record = new HashMap<>();
        type2record = new HashMap<>();
        jobid2record = new HashMap<>();
        recordList = new ArrayList<>();
    }

    /**
     * Gets the optimal clustering factor based on analysis
     *
     * @param d delay
     * @param a task failure rate monitored
     * @param t task runtime
     * @return optimal clustering factor
     */
    protected static double getK(double d, double a, double t) {
        double k = (-d + Math.sqrt(d * d - 4 * d / Math.log(1 - a))) / (2 * t);
        return k;
    }

    public static int getClusteringFactor(ContainerFailureRecord record) {

        double d = record.delayLength;

        double t = record.length;
        double a = 0.0;
        switch (ContainerFailureParameters.getMonitorMode()) {
            case MONITOR_JOB:
                /**
                 * not supported *
                 */
            case MONITOR_ALL:
                a = analyze(0, record.depth);
                break;
            case MONITOR_VM:
                a = analyze(0, record.vmId);
                break;
        }

        if (a <= 0.0) {
            return record.allTaskNum;
        } else {
            double k = getK(d, a, t);

            if (k <= 1) {
                k = 1;//minimal
            }

            return (int) k;
        }
    }

    public static void postFailureRecord(ContainerFailureRecord record) {

        if (record.workflowId < 0 || record.jobId < 0 || record.vmId < 0) {
            Log.printLine("Error in receiving failure record");
            return;
        }

        switch (ContainerFailureParameters.getMonitorMode()) {
            case MONITOR_VM:

                if (!vm2record.containsKey(record.vmId)) {
                    vm2record.put(record.vmId, new ArrayList<>());
                }
                vm2record.get(record.vmId).add(record);

                break;
            case MONITOR_JOB:

                if (!type2record.containsKey(record.depth)) {
                    type2record.put(record.depth, new ArrayList<>());
                }
                type2record.get(record.depth).add(record);

                break;
            case MONITOR_NONE:
                break;
        }

        recordList.add(record);
    }

    /**
     * Update the detected task failure rate based on record lists
     *
     * @param workflowId, doesn't work in this version
     * @param type,       the type of job or vm
     * @return task failure rate
     */
    public static double analyze(int workflowId, int type) {

        /**
         * workflow level : all jobs together *
         */
        int sumFailures = 0;
        int sumJobs = 0;
        switch (ContainerFailureParameters.getMonitorMode()) {
            case MONITOR_ALL:

                for (ContainerFailureRecord record : recordList) {
                    sumFailures += record.failedTasksNum;
                    sumJobs += record.allTaskNum;
                }

                break;

            case MONITOR_JOB:

                if (type2record.containsKey(type)) {
                    for (ContainerFailureRecord record : type2record.get(type)) {

                        sumFailures += record.failedTasksNum;
                        sumJobs += record.allTaskNum;
                    }
                }

                break;
            case MONITOR_VM:

                if (vm2record.containsKey(type)) {
                    for (ContainerFailureRecord record : vm2record.get(type)) {

                        sumFailures += record.failedTasksNum;
                        sumJobs += record.allTaskNum;
                    }
                }

                break;
        }


        if (sumFailures == 0) {
            return 0;
        }
        double alpha = (double) ((double) sumFailures / (double) sumJobs);
        return alpha;
    }

}
