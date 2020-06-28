package org.containerWorkflowsimِDemo.failure;

import org.cloudbus.cloudsim.Log;
import org.containerWorkflowsimِDemo.utils.ContainerDistributionGenerator;
import org.containerWorkflowsimِDemo.utils.ContainerDistributionGenerator.DistributionFamily;

public class ContainerFailureParameters {
    // TODO EHSAN failure for containers should be added
    private static ContainerDistributionGenerator[][] generators;
    public enum FTCluteringAlgorithm {

        FTCLUSTERING_DC, FTCLUSTERING_SR, FTCLUSTERING_DR, FTCLUSTERING_NOOP,
        FTCLUSTERING_BLOCK, FTCLUSTERING_VERTICAL
    }
    public enum FTCMonitor {

        MONITOR_NONE, MONITOR_ALL, MONITOR_VM, MONITOR_JOB, MONITOR_VM_JOB
    }
    public enum FTCFailure {

        FAILURE_NONE, FAILURE_ALL, FAILURE_VM, FAILURE_JOB, FAILURE_VM_JOB
    }

    private static FTCluteringAlgorithm FTClusteringAlgorithm =FTCluteringAlgorithm.FTCLUSTERING_NOOP;

    private static FTCMonitor monitorMode = FTCMonitor.MONITOR_NONE;

    private static FTCFailure failureMode = FTCFailure.FAILURE_NONE;

    private static DistributionFamily distribution = DistributionFamily.WEIBULL;

    private static final int INVALID = -1;

    public static void init(FTCluteringAlgorithm fMethod,FTCMonitor monitor,
                            FTCFailure failure, ContainerDistributionGenerator[][] failureGenerators) {
        FTClusteringAlgorithm = fMethod;
        monitorMode = monitor;
        failureMode = failure;
        generators = failureGenerators;
    }
    public static void init(FTCluteringAlgorithm fMethod,FTCMonitor monitor,
                            FTCFailure failure, ContainerDistributionGenerator[][] failureGenerators,
                            DistributionFamily dist) {
        distribution = dist;
        init(fMethod, monitor, failure, failureGenerators);
    }

    public static ContainerDistributionGenerator[][] getFailureGenerators() {
        if(generators==null){
            Log.printLine("ERROR: alpha is not initialized");
        }
        return generators;
    }

    public static int getFailureGeneratorsMaxFirstIndex(){
        if(generators==null || generators.length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return generators.length;
    }

    public static int getFailureGeneratorsMaxSecondIndex(){
        //Test whether it is valid
        getFailureGeneratorsMaxFirstIndex();
        if(generators[0]==null || generators[0].length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return generators[0].length;
    }

    public static ContainerDistributionGenerator getGenerator(int vmIndex, int taskDepth) {
        return generators[vmIndex][taskDepth];
    }

    public static FTCFailure getFailureGeneratorMode() {
        return failureMode;
    }


    public static FTCMonitor getMonitorMode() {
        return monitorMode;
    }


    public static FTCluteringAlgorithm getFTCluteringAlgorithm() {
        return FTClusteringAlgorithm;
    }

    public static DistributionFamily getFailureDistribution(){
        return distribution;
    }

}
