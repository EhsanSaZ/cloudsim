package org.mysim.deadlinedistribution;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.utils.Parameters;
import org.workflowsim.planning.HEFTPlanningAlgorithm;

import java.util.*;

public class DeadlineDistributionSimpleUpwardRank extends DeadlineDistributionStrategy {

    private Map<Task, Map<Integer, Double>> taskExecutionTimes;
    private Map<Task, Map<Task, Double>> taskTransferTimes;
    private Map<Task, Double> rank;
    private Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    public DeadlineDistributionSimpleUpwardRank() {
        taskExecutionTimes = new HashMap<>();
        taskTransferTimes = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
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
        // T ODO FIX: bug in calculation on make span.. what is finish time?
        double deadlineRemainingTime = wf.getDeadline() - wf.getCurrentMakeSpan();

        if (deadlineRemainingTime > 0 ){
            distributeDeadline(deadlineRemainingTime);
//            List<TaskRank> taskRank = new ArrayList<>();
//            for(Task t : wf.getTaskList()){
//                taskRank.add(new TaskRank(t, t.getRank()));
//            }
            // Sorting in non-ascending order of rank
//            Collections.sort(taskRank);
//            double rank_entry = taskRank.get(0).rank;

//            for (TaskRank tRank : taskRank) {
//                double averageExecutionTime = 0.0;
//
//                for (Double exeTime : taskExecutionTimes.get(tRank.task).values()) {
//                    averageExecutionTime += exeTime;
//                }
//                averageExecutionTime /= taskExecutionTimes.get(tRank.task).size();
//
//                double sub_deadline = ((rank_entry - tRank.rank) + averageExecutionTime) * remainingTime/ rank_entry;
//                tRank.task.setSubDeadline(sub_deadline);
//            }
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

    private void calculateRanks() {
        for (Task t : wf.getTaskList()) {
            calculateRank(t);
            t.setRank(rank.get(t));
        }
    }

    private double calculateRank(Task task) {
        if (rank.containsKey(task)) {
            return rank.get(task);
        }
        double averageExecutionTime = 0.0;

        for (Double exeTime : taskExecutionTimes.get(task).values()) {
            averageExecutionTime += exeTime;
        }
        averageExecutionTime /= taskExecutionTimes.get(task).size();

        double max = 0.0;
        for( Task child : task.getChildList()){
            double childCost = taskTransferTimes.get(task).get(child)
                    +calculateRank(child) ;
            max = Math.max(max, childCost);
        }
        rank.put(task, averageExecutionTime + max);

        return rank.get(task);
    }

    private class TaskRank implements Comparable<TaskRank>{
        public Task task;
        public Double rank;
        public TaskRank(Task task, Double rank) {
            this.task = task;
            this.rank = rank;
        }

        @Override
        public int compareTo(TaskRank o) {
            return o.rank.compareTo(rank);
        }
    }

    private  void distributeDeadline(double remainingDeadline){
        List<TaskRank> taskRank = new ArrayList<>();
        for(Task t : wf.getTaskList()){
            taskRank.add(new TaskRank(t, t.getRank()));
        }
        // Sorting in non-ascending order of rank
        Collections.sort(taskRank);
        double rank_entry = taskRank.get(0).rank;

        for (TaskRank tRank : taskRank) {
            double averageExecutionTime = 0.0;

            for (Double exeTime : taskExecutionTimes.get(tRank.task).values()) {
                averageExecutionTime += exeTime;
            }
            averageExecutionTime /= taskExecutionTimes.get(tRank.task).size();

            double sub_deadline = ((rank_entry - tRank.rank) + averageExecutionTime) * remainingDeadline/ rank_entry;
            tRank.task.setSubDeadline(sub_deadline);
        }
    }
    private void finishDistribution() {
//        Log.printConcatLine(CloudSim.clock(), ": DeadlineDistributor: Deadline distribution for workflow ", wf.getName(), " finished");
        averageBandwidth = -1;
        taskExecutionTimes.clear();
        taskTransferTimes.clear();
        rank.clear();
        earliestFinishTimes.clear();
        wf = null;
    }
}
