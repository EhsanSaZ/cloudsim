package org.containerWorkflowsimDemo.clustering;

import org.containerWorkflowsimDemo.ContainerFileItem;
import org.containerWorkflowsimDemo.ContainerJob;
import org.containerWorkflowsimDemo.ContainerTask;
import org.containerWorkflowsimDemo.utils.ContainerParameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * The default clustering does no clustering at all, just map a task to a tasks
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */

public class ContainerBasicClustering implements ContainerClusteringInterface {

    private List<ContainerTask> taskList;

    private final List<ContainerJob> jobList;

    private final Map mTask2Job;

    private final List<ContainerFileItem> allFileList;

    private ContainerTask root;

    private int idIndex;

    @Override
    public final List<ContainerFileItem> getTaskFiles() {
        return this.allFileList;
    }

    public ContainerBasicClustering() {
        this.jobList = new ArrayList<>();
        this.taskList = new ArrayList<>();
        this.mTask2Job = new HashMap<>();
        this.allFileList = new ArrayList<>();
        this.idIndex = 0;
        this.root = null;
    }

    @Override
    public final void setTaskList(List<ContainerTask> list) {
        this.taskList = list;
    }

    @Override
    public final List<ContainerJob> getJobList() {
        return this.jobList;
    }

    @Override
    public final List<ContainerTask> getTaskList() {
        return this.taskList;
    }

    public final Map getTask2Job() {
        return this.mTask2Job;
    }

    @Override
    public void run() {
        getTask2Job().clear();
        for (ContainerTask task : getTaskList()) {
            List<ContainerTask> list = new ArrayList<>();
            list.add(task);
            ContainerJob job = addTasks2Job(list);
            job.setVmId(task.getVmId());
            job.setContainerId(task.getContainerId());
            getTask2Job().put(task, job);
        }
        /**
         * Handle the dependencies issue.
         */
        updateDependencies();

    }
    protected final ContainerJob addTasks2Job(ContainerTask task) {
        List<ContainerTask> tasks = new ArrayList<>();
        tasks.add(task);
        return addTasks2Job(tasks);
    }

    protected final ContainerJob addTasks2Job(List<ContainerTask> taskList) {
        if (taskList != null && !taskList.isEmpty()) {
            int length = 0;
            double memory = 0;
            int userId = 0;
            int priority = 0;
            int depth = 0;
            /// a bug of cloudsim makes it final of input file size and output file size
            ContainerJob job = new ContainerJob(idIndex, length/*, inputFileSize, outputFileSize*/, memory);
            job.setClassType(ContainerParameters.ClassType.COMPUTE.value);
            for (ContainerTask task : taskList) {
                length += task.getCloudletLength();
                memory += task.getMemory();

                userId = task.getUserId();
                priority = task.getPriority();
                depth = task.getDepth();
                List<ContainerFileItem> fileList = task.getFileList();
                job.getContainerTaskList().add(task);

                getTask2Job().put(task, job);
                for (ContainerFileItem file : fileList) {
                    boolean hasFile = job.getFileList().contains(file);
                    if (!hasFile) {
                        job.getFileList().add(file);
                        if (file.getType() == ContainerParameters.FileType.INPUT) {
                            //for stag-in jobs to be used
                            if (!this.allFileList.contains(file)) {
                                this.allFileList.add(file);
                            }
                        } else if (file.getType() == ContainerParameters.FileType.OUTPUT) {
                            this.allFileList.add(file);
                        }
                    }
                }
                for (String fileName : task.getRequiredFiles()) {
                    if (!job.getRequiredFiles().contains(fileName)) {
                        job.getRequiredFiles().add(fileName);
                    }
                }
            }

            job.setCloudletLength(length);
            job.setMemory(memory);
            job.setUserId(userId);
            job.setDepth(depth);
            job.setPriority(priority);

            idIndex++;
            getJobList().add(job);
            return job;
        }
        return null;
    }

    /**
     * For a clustered tasks, we should add clustering delay (by default it is
     zero)
     */
    public void addClustDelay() {
        for (ContainerJob job : getJobList()) {
            double delay = ContainerParameters.getOverheadParams().getClustDelay(job);
            delay *= 1000; // the same ratio used when you parse a workflow
            long length = job.getCloudletLength();
            length += (long) delay;
            job.setCloudletLength(length);
        }
    }

    /**
     * Update the dependency issues between tasks/jobs
     */
    protected final void updateDependencies() {
        for (ContainerTask task : getTaskList()) {
            ContainerJob job = (ContainerJob) getTask2Job().get(task);
            for (ContainerTask parentTask : task.getParentList()) {
                ContainerJob parentJob = (ContainerJob) getTask2Job().get(parentTask);
                if (!job.getParentList().contains(parentJob) && parentJob != job) {//avoid dublicate
                    job.addParent(parentJob);
                }
            }
            for (ContainerTask childTask : task.getChildList()) {
                ContainerJob childJob = (ContainerJob) getTask2Job().get(childTask);
                if (!job.getChildList().contains(childJob) && childJob != job) {//avoid dublicate
                    job.addChild(childJob);
                }
            }
        }
        getTask2Job().clear();
        getTaskList().clear();
    }

    public ContainerTask addRoot() {
        if (root == null) {
            //bug maybe
            root = new ContainerTask(taskList.size() + 1, 0, 0/*,0,0*/);
            for (ContainerTask node : taskList) {
                if (node.getParentList().isEmpty()) {
                    node.addParent(root);
                    root.addChild(node);
                }
            }
            taskList.add(root);

        }
        return root;
    }
    /**
     * Delete the root task
     */
    public void clean() {
        if (root != null) {
            for (int i = 0; i < root.getChildList().size(); i++) {
                ContainerTask node = (ContainerTask) root.getChildList().get(i);
                node.getParentList().remove(root);
                root.getChildList().remove(node);
                i--;
            }
            taskList.remove(root);
        }
    }
}
