package org.containerWorkflowsimِDemo.reclustering;

import org.cloudbus.cloudsim.Cloudlet;
import org.containerWorkflowsimِDemo.ContainerJob;
import org.containerWorkflowsimِDemo.failure.ContainerFailureParameters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ContainerReclusteringEngine {
    private static ContainerJob createJob(int id, ContainerJob job, long length, double peak_memory, List taskList, boolean updateDep) {
        try {
            ContainerJob newJob = new ContainerJob(id, length, peak_memory);
            newJob.setUserId(job.getUserId());
            newJob.setVmId(-1);
            newJob.setCloudletStatus(Cloudlet.CREATED);
            newJob.setContainerTaskList(taskList);
            newJob.setDepth(job.getDepth());

            if (updateDep) {
                newJob.setChildList(job.getChildList());
                newJob.setParentList(job.getParentList());
                for (Iterator it = job.getChildList().iterator(); it.hasNext();) {
                    ContainerJob cJob = (ContainerJob) it.next();
                    cJob.addParent(newJob);
                }
            }
            return newJob;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<ContainerJob> process(ContainerJob job, int id) {
        List jobList = new ArrayList();
        try {
            switch (ContainerFailureParameters.getFTCluteringAlgorithm()) {
                case FTCLUSTERING_NOOP:
                    jobList.add(createJob(id, job, job.getCloudletLength(), job.getMemory(), job.getContainerTaskList(), true));
                    //job submttted doesn't have to be considered
                    break;
//                /**
//                 * Dynamic clustering.
//                 */
//                case FTCLUSTERING_DC:
//                    jobList = DCReclustering(jobList, job, id, job.getTaskList());
//                    break;
//                /**
//                 * Selective reclustering.
//                 */
//                case FTCLUSTERING_SR:
//                    jobList = SRReclustering(jobList, job, id);
//
//                    break;
//                /**
//                 * Dynamic reclustering.
//                 */
//                case FTCLUSTERING_DR:
//                    jobList = DRReclustering(jobList, job, id, job.getTaskList());
//                    break;
//                /**
//                 * Block reclustering.
//                 */
//                case FTCLUSTERING_BLOCK:
//                    jobList = BlockReclustering(jobList, job, id);
//                    break;
//                /**
//                 * Binary reclustering.
//                 */
//                case FTCLUSTERING_VERTICAL:
//                    jobList = VerticalReclustering(jobList, job, id);
//                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobList;
    }
    // TODO EHSAN more reclustering methods should be added from re-clustering  in workflow sim
}
