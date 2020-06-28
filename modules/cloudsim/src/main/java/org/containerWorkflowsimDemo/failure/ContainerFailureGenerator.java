package org.containerWorkflowsimDemo.failure;

import org.apache.commons.math3.distribution.*;
import org.cloudbus.cloudsim.Cloudlet;
import org.containerWorkflowsimDemo.ContainerJob;
import org.containerWorkflowsimDemo.ContainerTask;

import org.containerWorkflowsimDemo.utils.ContainerDistributionGenerator;
public class ContainerFailureGenerator {
    // TODO EHSAN failure for containers should be added
    private static final int maxFailureSizeExtension = 50;
    private static int failureSizeExtension = 0;
    private static final boolean hasChangeTime = false;

    protected static RealDistribution getDistribution(double alpha, double beta) {
        RealDistribution distribution = null;
        switch (ContainerFailureParameters.getFailureDistribution()) {
            case LOGNORMAL:
                distribution = new LogNormalDistribution(1.0 / alpha, beta);
                break;
            case WEIBULL:
                distribution = new WeibullDistribution(beta, 1.0 / alpha);
                break;
            case GAMMA:
                distribution = new GammaDistribution(beta, 1.0 / alpha);
                break;
            case NORMAL:
                //beta is the std, 1.0/alpha is the mean
                distribution = new NormalDistribution(1.0 / alpha, beta);
                break;
            default:
                break;
        }
        return distribution;
    }

    protected static void initFailureSamples() {
    }

    public static void init() {

        initFailureSamples();
    }

    protected static boolean checkFailureStatus(ContainerTask task, int vmId) throws Exception {


        ContainerDistributionGenerator generator;
        switch (ContainerFailureParameters.getFailureGeneratorMode()) {
            /**
             * Every task follows the same distribution.
             */
            case FAILURE_ALL:
                generator = ContainerFailureParameters.getGenerator(0, 0);
                break;
            /**
             * Generate failures based on the type of job.
             */
            case FAILURE_JOB:
                generator = ContainerFailureParameters.getGenerator(0, task.getDepth());
                break;
            /**
             * Generate failures based on the index of vm.
             */
            case FAILURE_VM:
                generator = ContainerFailureParameters.getGenerator(vmId, 0);
                break;
            /**
             * Generator failures based on vmId and level both
             */
            case FAILURE_VM_JOB:
                generator = ContainerFailureParameters.getGenerator(vmId, task.getDepth());
                break;
            default:
                return false;
        }

        double start = task.getExecStartTime();
        double end = task.getTaskFinishTime();


        double[] samples = generator.getCumulativeSamples();

        while (samples[samples.length - 1] < start) {
            generator.extendSamples();
            samples = generator.getCumulativeSamples();
            failureSizeExtension++;
            if (failureSizeExtension >= maxFailureSizeExtension) {
                throw new Exception("Error rate is too high such that the simulator terminates");

            }
        }

        for (int sampleId = 0; sampleId < samples.length; sampleId++) {
            if (end < samples[sampleId]) {
                //no failure
                return false;
            }
            if (start <= samples[sampleId]) {
                //has a failure
                /** The idea is we need to update the cursor in generator**/
                generator.getNextSample();
                return true;
            }
        }

        return false;
    }
    public static boolean generate(ContainerJob job) {
        boolean jobFailed = false;
        if (ContainerFailureParameters.getFailureGeneratorMode() == ContainerFailureParameters.FTCFailure.FAILURE_NONE) {
            return jobFailed;
        }
        try {

            for (ContainerTask task : job.getContainerTaskList()) {
                int failedTaskSum = 0;
                if (checkFailureStatus(task, job.getVmId())) {
                    //this task fail
                    jobFailed = true;
                    failedTaskSum++;
                    task.setCloudletStatus(Cloudlet.FAILED);
                }
                ContainerFailureRecord record = new ContainerFailureRecord(0, failedTaskSum, task.getDepth(), 1, job.getVmId(), task.getCloudletId(), job.getUserId(), task.getContainerId());
                ContainerFailureMonitor.postFailureRecord(record);
            }

            if (jobFailed) {
                job.setCloudletStatus(Cloudlet.FAILED);
            } else {
                job.setCloudletStatus(Cloudlet.SUCCESS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobFailed;
    }


}
