package org.containerWorkflowsimِDemo;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ehsan on 6/6/2020.
 */
public class ContainerTask extends ContainerCloudlet {
    /*
     * The list of parent tasks.
     */
    private List<ContainerTask> parentList;
    /*
     * The list of child tasks.
     */
    private List<ContainerTask> childList;
    /*
     * The list of all files (input data and ouput data)
     */
    private List<ContainerFileItem> fileList;
    /*
     * The priority used for research. Not used in current version.
     */
    private int priority;
    /*
     * The depth of this task. Depth of a task is defined as the furthest path
     * from the root task to this task. It is set during the workflow parsing
     * stage.
     */
    private int depth;
    /*
     * The impact of a task. It is used in research.
     */
    private double impact;

    /*
     * The type of a task.
     */
    private String type;

    /*
     * The finish time of a task (Because cloudlet does not allow WorkflowSim to
     * update finish_time)
     */
    private double taskFinishTime;
    /*
     * The memory of a task.
     */
    private double memory;

    /**
     * Allocates a new Task object. The task length should be greater than or
     * equal to 1.
     *
     * @param taskId     the unique ID of this Task
     * @param taskLength the length or size (in MI) of this task to be executed
     *                   in a PowerDatacenter
     * @pre taskId >= 0
     * @pre taskLength >= 0.0
     * @post $none
     */
    public ContainerTask(final int taskId, final long taskLength, double peak_memory) {
        /**
         * We do not use cloudletFileSize and cloudletOutputSize here. We have
         * added a list to task and thus we don't need a cloudletFileSize or
         * cloudletOutputSize here The utilizationModelCpu, utilizationModelRam,
         * and utilizationModelBw are just set to be the default mode. You can
         * change it for your own purpose.
         */
        super(taskId, taskLength, 1, 0, 0, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
    }
    public ContainerTask(final int taskId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                         UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
    }

    public ContainerTask(final int taskId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                         UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                         boolean record, List<String> fileList) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, record, fileList);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
    }

    public ContainerTask(final int taskId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                         UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                         boolean record) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, record);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
    }

    public ContainerTask(final int taskId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                         UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                         List<String> fileList) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
    }


    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
    public int getPriority() {
        return this.priority;
    }


    public void setDepth(int depth) {
        this.depth = depth;
    }
    public int getDepth() {
        return this.depth;
    }



    public List<ContainerTask> getChildList() {
        return this.childList;
    }
    public void setChildList(List<ContainerTask> list) {
        this.childList = list;
    }


    public List<ContainerTask> getParentList() {
        return this.parentList;
    }
    public void setParentList(List<ContainerTask> list) {
        this.parentList = list;
    }


    public void addChildList(List<ContainerTask> list) {
        this.childList.addAll(list);
    }
    public void addParentList(List<ContainerTask> list) {
        this.parentList.addAll(list);
    }


    public void addChild(ContainerTask task) {
        this.childList.add(task);
    }
    public void addParent(ContainerTask task) {
        this.parentList.add(task);
    }

    public List<ContainerFileItem> getFileList() {
        return this.fileList;
    }
    public void setFileList(List<ContainerFileItem> list) {
        this.fileList = list;
    }

    public void addFile(ContainerFileItem file) {
        this.fileList.add(file);
    }

    public void setImpact(double impact) {
        this.impact = impact;
    }
    public double getImpact() {
        return this.impact;
    }

    public void setTaskFinishTime(double time) { this.taskFinishTime = time; }
    public double getTaskFinishTime() { return this.taskFinishTime; }

    public void setMemory(double memory){this.memory = memory;}
    public double getMemory() { return this.memory; }

    /**
     * Gets the total cost of processing or executing this task The original
     * getProcessingCost does not take cpu cost into it also the data file in
     * Task is stored in fileList <tt>Processing Cost = input data transfer +
     * processing cost + output transfer cost</tt> .
     *
     * @return the total cost of processing Cloudlet
     * @pre $none
     * @post $result >= 0.0
     */
    //TODO Ehsan: change and calculate new cost...
    @Override
    public double getProcessingCost() {
        // cloudlet cost: execution cost...

        double cost = getCostPerSec() * getActualCPUTime();

        // ...plus input data transfer cost...
        long fileSize = 0;
        for (ContainerFileItem file : getFileList()) {
            fileSize += file.getSize() / Consts.MILLION;
        }
        cost += costPerBw * fileSize;
        return cost;
    }

}
