package org.mysim.deadlinedistribution;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.utils.Parameters;

import java.util.*;

public class RankAndDeadlineDistributionMWHBDCSAlgorithm extends DeadlineDistributionStrategy {

    private Map<Task, Map<Integer, Double>> taskExecutionTimes;
    private Map<Task, Map<Task, Double>> taskTransferTimes;
    private Map<Task, Double> rank;
    private Map<Task, Double> earliestFinishTimes;
    private Map<Task, Double> timePjMap;
    private Map<Task, Map<Integer, Double>> taskT2CjMap;

    private double averageBandwidth;

    public RankAndDeadlineDistributionMWHBDCSAlgorithm() {
        taskExecutionTimes = new HashMap<>();
        taskTransferTimes = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
        timePjMap = new HashMap<>();
        taskT2CjMap = new HashMap<>();
    }

    @Override
    public void run() {
        if (wf != null) {
//            Log.printConcatLine(CloudSim.clock(), ": DeadlineDistributor: Start DeadlineDistribution for workflow ", wf.getName(),
//                    " and with ", wf.getTaskList().size() , " tasks.");

            averageBandwidth = Parameters.VM_BW;

            calculateTaskExecutionTime();
            calculateTransferTimes();
            calculateRanks();

            distributeDeadline(wf.getDeadline());
            finishDistribution();
        } else {
            Log.printConcatLine(CloudSim.clock(),": DeadlineDistributor: No workflow set for deadline distribution" );
        }

    }

    @Override
    public void updateSubDeadlines(){
        if (wf != null) {
//            Log.printConcatLine(CloudSim.clock(), ": DeadlineDistributor: Start updating sub-deadlines for workflow " , wf.getName() ,
//                    " and with " , wf.getTaskList().size() , " tasks.");
            averageBandwidth = Parameters.VM_BW;
            calculateTaskExecutionTime();
            calculateTransferTimes();
            updateSubDeadlinesDistribution();
            finishDistribution();

        } else {
            Log.printConcatLine(CloudSim.clock(),": DeadlineDistributor: No workflow set for update deadline distribution" );

        }
    }
    private void updateSubDeadlinesDistribution(){
        double deadlineRemainingTime = wf.getDeadline() - wf.getCurrentMakeSpan();
        if (deadlineRemainingTime > 0 ){
            distributeDeadline(deadlineRemainingTime);
        }else{
            // option 1 reject whole workflow as failure
            // option 2 user fastest time amon all vms as deadline... selected for now..
            for(Task t : wf.getTaskList()){
                t.setSubDeadline(taskExecutionTimes.get(t).get(Parameters.VM_TYPES_NUMBERS-1));
            }
        }
    }
    private void calculateTaskExecutionTime() {
        for (Task t : wf.getTaskList()) {
            Map<Integer, Double> exeTimeMap = new HashMap<>();
            for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++) {
                exeTimeMap.put(i, t.getTaskTotalLength() / (Parameters.VM_MIPS[i] * Parameters.VM_PES[i]));
            }
            taskExecutionTimes.put(t, exeTimeMap);
        }
    }

    // transfer time from a parent task to each child in taskTransferTimes map
    private void calculateTransferTimes() {
        // Initializing the matrix
        for (Task t1 : wf.getTaskList()) {
            Map<Task, Double> taskTransTimes = new HashMap<>();
            for (Task t2 : wf.getTaskList()) {
                taskTransTimes.put(t2, 0.0);
            }
            taskTransferTimes.put(t1, taskTransTimes);
        }
        // Calculating the actual values
        for (Task parent : wf.getTaskList()) {
            for (Task child : parent.getChildList()) {
                taskTransferTimes.get(parent).put(child,
                        calculateTransferTime(parent, child));

            }
        }
    }

    private double calculateTransferTime(Task parent, Task child) {
        List<FileItem> parentFiles = parent.getFileList();
        List<FileItem> childFiles = child.getFileList();

        double acc = 0.0;

        for (FileItem parentFile : parentFiles) {
            if (parentFile.getType() != Parameters.FileType.OUTPUT) {
                continue;
            }
            for (FileItem childFile : childFiles) {
                if (childFile.getType() == Parameters.FileType.INPUT
                        && childFile.getName().equals(parentFile.getName())) {
                    acc += childFile.getSize();
                    break;
                }
            }
        }
        // T ODO EHSAN: convert filesize to MB 1024*1024
        //file Size is in Bytes, acc in MB
        acc = acc / Consts.MILLION;
        // acc in MB, averageBandwidth in Mb/s
        return acc * 8 / averageBandwidth;
    }

    private double calculateTaskExecutionCostOnVmType(Task task, int vmTypeIndex){
        double exe_time = (task.getTaskTotalLength() / (Parameters.VM_MIPS[vmTypeIndex] * Parameters.VM_PES[vmTypeIndex]) )
                + task.getTransferTime(Parameters.VM_BW);
        return  Parameters.COST[vmTypeIndex] * Math.ceil( exe_time / Parameters.BILLING_PERIOD);
    }

    private void calculateTimePjs(){
        for (Task t : wf.getTaskList()) {
            calculateTimePj(t);
        }
    }
    private double calculateTimePj(Task task){
        if (timePjMap.containsKey(task)){
            return timePjMap.get(task);
        }
        double maxValue = 0.0;
        for(Task parentT: task.getParentList()){
            double minExecutionTime = Collections.min(taskExecutionTimes.get(task).values());
//            for (Double exeTime : taskExecutionTimes.get(task).values()) {
//                minExecutionTime = Math.min(minExecutionTime, exeTime);
//            }
            maxValue = Math.max(maxValue,calculateTimePj(parentT) + minExecutionTime + taskTransferTimes.get(parentT).get(task));
        }

        timePjMap.put(task, maxValue);
        return timePjMap.get(task);
    }

    private void populateT2CMap(){
        calculateTimePjs();
        for (Task task: wf.getTaskList()){
            double currentTaskTimePj = timePjMap.get(task);
            double currentCostPj = 0.0;
            for (Task t2 : timePjMap.keySet()){
                if (timePjMap.get(t2) <= currentTaskTimePj){
                    double minCost = Double.MAX_VALUE;
                    for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
                        minCost = Math.min(minCost, calculateTaskExecutionCostOnVmType(t2, i));
                    }
                    currentCostPj += minCost;
                }
            }
            Map<Integer, Double> T2CjMap = new HashMap<>();

            for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
                double RCTjk = (wf.getDeadline() - currentTaskTimePj)
                        * ( calculateTaskExecutionCostOnVmType(task, i) / (wf.getBudget() - currentCostPj) );
                T2CjMap.put(i,  taskExecutionTimes.get(task).get(i) + RCTjk);
            }

            taskT2CjMap.put(task, T2CjMap);
        }
    }

    private void calculateRanks() {
        populateT2CMap();
        for (Task t : wf.getTaskList()) {
            calculateRank(t);
            t.setRank(rank.get(t));
        }
    }
    private double calculateRank(Task task) {
        if (rank.containsKey(task)) {
            return rank.get(task);
        }

        double minT2C =  Collections.min(taskT2CjMap.get(task).values());
//        for (double t2c: taskT2CjMap.get(task).values()){
//            minT2C = Math.min(minT2C, t2c);
//        }
        double max = 0.0;
        for( Task child : task.getChildList()){
            max = Math.max(max, taskTransferTimes.get(task).get(child) + calculateRank(child));
        }
        rank.put(task, minT2C + max);

        return rank.get(task);
    }

    private double calculateSubDeadline(Task task){
        if (task.getChildList().size() == 0){
            task.setSubDeadline(wf.getDeadline());
            return task.getSubDeadline();
        }
        double minSubDeadline = wf.getDeadline();
        for (Task child: task.getChildList()){
            minSubDeadline = Math.min(minSubDeadline,
                                        calculateSubDeadline(child)
                                        - taskTransferTimes.get(task).get(child)
                                        - Collections.max(taskExecutionTimes.get(child).values())
                                        );
        }
        task.setSubDeadline(minSubDeadline);
        return task.getSubDeadline();
    }

    private  void distributeDeadline(double remainingDeadline){
        for (Task task : wf.getTaskList()){
            calculateSubDeadline(task);
        }
    }
    private void finishDistribution() {
//        Log.printConcatLine(CloudSim.clock(), ": DeadlineDistributor: Deadline distribution for workflow ", wf.getName(), " finished");
        averageBandwidth = -1;
        taskExecutionTimes.clear();
        taskTransferTimes.clear();
        rank.clear();
        earliestFinishTimes.clear();
        timePjMap.clear();
        taskT2CjMap.clear();
        wf = null;
    }
}
