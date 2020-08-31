package org.mysim;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.mysim.utils.Parameters;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ehsan on 6/6/2020.
 */
public class Task extends ContainerCloudlet {

    private List<Task> parentList;

    private List<Task> childList;

    private List<FileItem> fileList;

    private int priority;

    private int depth;

    private double impact;

    private String type;

    private double taskFinishTime;

    private double memory;

    private double subDeadline;

    private double subBudget;

    private double taskExecutionTime;

    private double taskExecutionCost;

    private int workflowID;

    private double rank;

    private double allocatedVmTotalMips;
    private double allocatedVmRam;

    private double allocatedContainerTotalMips;
    private double allocatedContainerRam;

    private long taskTotalLength;

    public Task(final int taskId, int workflowId, final long taskLength, double peak_memory) {
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
        this.subDeadline = -1;
        this.subBudget = -1;
        this.taskExecutionCost = -1;
        this.workflowID = workflowId;
        this.allocatedVmTotalMips = -1;
        this.allocatedVmRam = -1;
        this.allocatedContainerTotalMips = -1;
        this.allocatedContainerRam = -1;
        this.taskTotalLength = taskLength;

    }
    public Task(final int taskId,int workflowId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
        this.subDeadline = -1;
        this.subBudget = -1;
        this.taskExecutionCost = -1;
        this.workflowID = workflowId;
        this.allocatedVmTotalMips = -1;
        this.allocatedVmRam = -1;
        this.allocatedContainerTotalMips = -1;
        this.allocatedContainerRam = -1;
        this.taskTotalLength = taskLength;
    }

    public Task(final int taskId,int workflowId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                boolean record, List<String> fileList) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, record, fileList);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
        this.subDeadline = -1;
        this.subBudget = -1;
        this.taskExecutionCost = -1;
        this.workflowID = workflowId;
        this.allocatedVmTotalMips = -1;
        this.allocatedVmRam = -1;
        this.allocatedContainerTotalMips = -1;
        this.allocatedContainerRam = -1;
        this.taskTotalLength = taskLength;
    }

    public Task(final int taskId, int workflowId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                boolean record) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, record);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
        this.subDeadline = -1;
        this.subBudget = -1;
        this.taskExecutionCost = -1;
        this.workflowID = workflowId;
        this.allocatedVmTotalMips = -1;
        this.allocatedVmRam = -1;
        this.allocatedContainerTotalMips = -1;
        this.allocatedContainerRam = -1;
        this.taskTotalLength = taskLength;
    }

    public Task(final int taskId,int workflowId, final long taskLength, double peak_memory, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                List<String> fileList) {
        super(taskId, taskLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
        this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.memory = peak_memory;
        this.subDeadline = -1;
        this.subBudget = -1;
        this.taskExecutionCost = -1;
        this.workflowID = workflowId;
        this.allocatedVmTotalMips = -1;
        this.allocatedVmRam = -1;
        this.allocatedContainerTotalMips = -1;
        this.allocatedContainerRam = -1;
        this.taskTotalLength = taskLength;
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



    public List<Task> getChildList() {
        return this.childList;
    }
    public void setChildList(List<Task> list) {
        this.childList = list;
    }


    public List<Task> getParentList() {
        return this.parentList;
    }
    public void setParentList(List<Task> list) {
        this.parentList = list;
    }


    public void addChildList(List<Task> list) {
        this.childList.addAll(list);
    }
    public void addParentList(List<Task> list) {
        this.parentList.addAll(list);
    }


    public void addChild(Task task) {
        this.childList.add(task);
    }
    public void addParent(Task task) {
        this.parentList.add(task);
    }

    public List<FileItem> getFileList() {
        return this.fileList;
    }
    public void setFileList(List<FileItem> list) {
        this.fileList = list;
    }

    public void addFile(FileItem file) {
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

    //T ODO Ehsan: change and calculate new cost...
    @Override
    public double getProcessingCost() {
        if (getDepth() == 0){
            return  0.0;
        }
        // cloudlet cost: execution cost...
        double actualCPUTime = getActualCPUTime();
        double costpersecond = getCostPerSec();
//        double relativeCostRate = getCostPerSec() * (Parameters.CPU_COST_FACTOR * (getAllocatedContainerTotalMips() / getAllocatedVmTotalMips()) +
//                                                    (1 - Parameters.CPU_COST_FACTOR) * (getAllocatedContainerRam() / getAllocatedVmRam())
//                                                    );
        double relativeCostRate = getCostPerSec() * ( getAllocatedContainerTotalMips() / getAllocatedVmTotalMips());

        double cost = relativeCostRate * Math.ceil( getActualCPUTime() / Parameters.BILLING_PERIOD);

        // ...plus input data transfer cost...
//        long fileSize = 0;
//        for (FileItem file : getFileList()) {
//            fileSize += file.getSize() / Consts.MILLION;
//        }
//        cost += costPerBw * fileSize;
        return cost;
    }

    public double getTransferTime(int bw){
        double transferTime = 0.0;
        if (getClassType() == Parameters.ClassType.COMPUTE.value){
            for (FileItem file: getFileList()){
                if (file.isRealInputFile(getFileList()) || file.isRealOutputFile(getFileList())){
                    transferTime += file.getSize()  * 8 / (double) Consts.MILLION / bw;
                }
            }
        }
        return transferTime;
    }

    public double estimateTaskCost (ContainerVm vm){
        CondorVM castedVm = (CondorVM) vm;

//        double relativeCostRate = castedVm.getCost() * ( Parameters.CPU_COST_FACTOR * ((double)getNumberOfPes() / vm.getPeList().size()) +
//                ( 1 - Parameters.CPU_COST_FACTOR) * (getMemory()/ castedVm.getRam())
//        );
//        double executionTime = ( getCloudletLength() / (Parameters.VM_MIPS[0] * getNumberOfPes()) )+ getTransferTime(Parameters.VM_BW);
        int[] config = getFutureContainerConfig(vm);
        double relativeCostRate = castedVm.getCost() * ((double)config[0] / vm.getNumberOfPes());
        double executionTime = ( getTaskTotalLength() / (Parameters.VM_MIPS[0] * config[0]) )+ getTransferTime(Parameters.VM_BW);
        return relativeCostRate * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
    }

    public double estimateTaskCostForVmType(int vmType){

//        double relativeCostRate =  Parameters.COST[vmType] * ( Parameters.CPU_COST_FACTOR * ((double)getNumberOfPes() / Parameters.VM_PES[vmType]) +
//                ( 1 - Parameters.CPU_COST_FACTOR) * (getMemory()/ Parameters.VM_RAM[vmType])
//        );
//        double executionTime = ( getCloudletLength() / (Parameters.VM_MIPS[0] * getNumberOfPes()) )+ getTransferTime(Parameters.VM_BW);
        int[] config = getFutureContainerConfigForVmType(vmType);
        double relativeCostRate = Parameters.COST[vmType] *((double)config[0] / Parameters.VM_PES[vmType]);
        double executionTime = ( getTaskTotalLength() / (Parameters.VM_MIPS[0] * config[0]) )+ getTransferTime(Parameters.VM_BW);
        return relativeCostRate * Math.ceil( executionTime / Parameters.BILLING_PERIOD);
    }

    public boolean isVmAffordable(ContainerVm vm){
        return (estimateTaskCost(vm) < getSubBudget());
    }

    public int[] getFutureContainerConfig(ContainerVm vm){
        int[] config = new int[2];
        config[0] = getNumberOfPes();
        config[1] = (int)Math.ceil(getMemory());

        CondorVM castedVm = (CondorVM) vm;
        double ratio = Math.max( (double) config[0] / castedVm.getNumberOfPes(), config[1] / castedVm.getRam());
        config[0] = (int) Math.ceil( ratio * castedVm.getNumberOfPes());
        config[1] = (int) (config[0] * castedVm.getMemPerCore());

        return config;
    }

    public int[] getFutureContainerConfigForVmType(int vmType){
        int[] config = new int[2];
        config[0] = getNumberOfPes();
        config[1] = (int)Math.ceil(getMemory());

        double ratio = Math.max( (double) config[0] / Parameters.VM_PES[vmType], config[1] / Parameters.VM_RAM[vmType]);
        config[0] = (int) Math.ceil( ratio * Parameters.VM_PES[vmType]);
        config[1] = (int) (config[0] * (Parameters.VM_RAM[vmType] / Parameters.VM_PES[vmType]));

        return config;
    }

    public void updateCoudletLength(int peNumbers){
        // bug of cloudsim min length must be 110
        setCloudletLength(Math.max(getTaskTotalLength() / getNumberOfPes(), 110));
    }

    //-------------------------------------- setter and getter ----------------------------------
    public double getSubDeadline() {
        return subDeadline;
    }

    public void setSubDeadline(double subDeadline) {
        this.subDeadline = subDeadline;
    }

    public double getSubBudget() {
        return subBudget;
    }

    public void setSubBudget(double subBudget) {
        this.subBudget = subBudget;
    }

    public double getTaskExecutionTime() {
        return taskExecutionTime;
    }

    public void setTaskExecutionTime(double taskExecutionTime) {
        this.taskExecutionTime = taskExecutionTime;
    }

    public double getTaskExecutionCost() {
        return taskExecutionCost;
    }

    public void setTaskExecutionCost(double taskExecutionCost) {
        this.taskExecutionCost = taskExecutionCost;
    }

    public int getWorkflowID() {
        return workflowID;
    }

    public void setWorkflowID(int workflowID) {
        this.workflowID = workflowID;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public double getAllocatedVmTotalMips() { return allocatedVmTotalMips; }

    public void setAllocatedVmTotalMips(double allocatedVmTotalMips) { this.allocatedVmTotalMips = allocatedVmTotalMips; }

    public double getAllocatedVmRam() { return allocatedVmRam; }

    public void setAllocatedVmRam(double allocatedVmRam) { this.allocatedVmRam = allocatedVmRam; }

    public double getAllocatedContainerTotalMips() { return allocatedContainerTotalMips; }

    public void setAllocatedContainerTotalMips(double allocatedContainerTotalMips) { this.allocatedContainerTotalMips = allocatedContainerTotalMips; }

    public double getAllocatedContainerRam() { return allocatedContainerRam; }

    public void setAllocatedContainerRam(double allocatedContainerRam) { this.allocatedContainerRam = allocatedContainerRam; }

    public long getTaskTotalLength() {
        return taskTotalLength;
    }

    public void setTaskTotalLength(long taskTotalLength) {
        this.taskTotalLength = taskTotalLength;
    }
}
