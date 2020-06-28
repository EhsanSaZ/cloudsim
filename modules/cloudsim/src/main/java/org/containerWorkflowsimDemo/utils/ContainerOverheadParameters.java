package org.containerWorkflowsimDemo.utils;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.containerWorkflowsimDemo.ContainerJob;

import java.util.List;
import java.util.Map;

public class ContainerOverheadParameters {
    private final int WED_INTERVAL;

    private final double bandwidth;

    private final Map<Integer, ContainerDistributionGenerator> WED_DELAY;

    private final Map<Integer, ContainerDistributionGenerator> QUEUE_DELAY;

    private final Map<Integer, ContainerDistributionGenerator> POST_DELAY;

    private final Map<Integer, ContainerDistributionGenerator> CLUST_DELAY;
    public ContainerOverheadParameters(int wed_interval,
                              Map<Integer, ContainerDistributionGenerator> wed_delay,
                              Map<Integer, ContainerDistributionGenerator> queue_delay,
                              Map<Integer, ContainerDistributionGenerator> post_delay,
                              Map<Integer, ContainerDistributionGenerator> cluster_delay,
                              double bandwidth) {
        this.WED_INTERVAL = wed_interval;
        this.WED_DELAY = wed_delay;
        this.QUEUE_DELAY = queue_delay;
        this.POST_DELAY = post_delay;
        this.CLUST_DELAY = cluster_delay;
        this.bandwidth = bandwidth;

    }
    public double getBandwidth() {
        return this.bandwidth;
    }
    public int getWEDInterval() {
        return this.WED_INTERVAL;
    }

    public Map<Integer, ContainerDistributionGenerator> getQueueDelay() {
        return this.QUEUE_DELAY;
    }

    public Map<Integer, ContainerDistributionGenerator> getPostDelay() {
        return this.POST_DELAY;
    }

    public Map<Integer, ContainerDistributionGenerator> getWEDDelay() {
        return this.WED_DELAY;
    }

    public Map<Integer, ContainerDistributionGenerator> getClustDelay() {
        return this.CLUST_DELAY;
    }

    public double getClustDelay(ContainerCloudlet cl) {
        double delay = 0.0;
        if(this.CLUST_DELAY == null){
            return delay;
        }
        if (cl != null) {
            ContainerJob job = (ContainerJob) cl;

            if (this.CLUST_DELAY.containsKey(job.getDepth())) {
                delay = this.CLUST_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.CLUST_DELAY.containsKey(0)) {
                delay = this.CLUST_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }


        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    public double getQueueDelay(ContainerCloudlet cl) {
        double delay = 0.0;

        if(this.QUEUE_DELAY == null){
            return delay;
        }
        if (cl != null) {
            ContainerJob job = (ContainerJob) cl;

            if (this.QUEUE_DELAY.containsKey(job.getDepth())) {
                delay = this.QUEUE_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.QUEUE_DELAY.containsKey(0)) {
                delay = this.QUEUE_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }


        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    public double getPostDelay(ContainerJob job) {
        double delay = 0.0;

        if(this.POST_DELAY == null){
            return delay;
        }
        if (job != null) {

            if (this.POST_DELAY.containsKey(job.getDepth())) {
                delay = this.POST_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.POST_DELAY.containsKey(0)) {
                //the default one
                delay = this.POST_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }

        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    public double getWEDDelay(List list) {
        double delay = 0.0;

        if(this.WED_DELAY == null){
            return delay;
        }
        if (!list.isEmpty()) {
            ContainerJob job = (ContainerJob) list.get(0);
            if (this.WED_DELAY.containsKey(job.getDepth())) {
                delay = this.WED_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.WED_DELAY.containsKey(0)) {
                delay = this.WED_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }

        } else {
            //actuall set it to be 0.0;
            Log.printLine("Not yet supported");
        }
        return delay;
    }
}
