package org.mysim.deadlinedistribution;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.utils.Parameters;

import java.util.*;

public class EPSMDeadlineDistributionAlgorithm extends DeadlineDistributionStrategy {

    private Map<Task, Map<Integer, Double>> taskExecutionTimes;
    private Map<Task, Map<Task, Double>> taskTransferTimes;
    private Map<Task, Double> rank;
    private Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    public EPSMDeadlineDistributionAlgorithm() {
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
            double deadlineRemainingTime = wf.getDeadline() - wf.getCurrentMakeSpan();
            if (deadlineRemainingTime > 0 ){
                distributeDeadline(deadlineRemainingTime);
            }else{
                // option 1 reject whole workflow as failure
                // option 2 user fastest time amon all vms as deadline... selected for now..
                for(Task t : wf.getTaskList()){
                    t.setSubDeadline(taskExecutionTimes.get(t).get(Parameters.VM_TYPES_NUMBERS-1)
                            + t.getTransferTime(Parameters.VM_BW));
                }
            }
            finishDistribution();

        } else {
            Log.printConcatLine(CloudSim.clock(),": DeadlineDistributor: No workflow set for update deadline distribution" );

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

    private double getEarliestFinishTime(Task task, int VMType){
        if(earliestFinishTimes.containsKey(task)){
            return earliestFinishTimes.get(task);
        }
        double maxParentEFT = 0.0;
        for(Task parentT: task.getParentList()){
            if (wf.getTaskList().contains(parentT)){
                maxParentEFT = Math.max(maxParentEFT, getEarliestFinishTime(parentT, VMType));
            }
        }
        earliestFinishTimes.put(task,
                maxParentEFT + taskExecutionTimes.get(task).get(VMType) + task.getTransferTime(Parameters.VM_BW));
        return earliestFinishTimes.get(task);
    }
    private  void distributeDeadline(double remainingDeadline){
        double makeSpan = 0;
        int chosenType = 0;
        for (int VMType = 0; VMType < Parameters.VM_TYPES_NUMBERS; VMType++){
            makeSpan = -Double.MAX_VALUE;
            for (Task t:wf.getTaskList()){
                makeSpan = Math.max(makeSpan, getEarliestFinishTime(t, VMType));
            }
            if(makeSpan < remainingDeadline){
                chosenType = VMType;
                break;
            }
        }
        if(remainingDeadline - makeSpan > 0){
            double totalSpareTime = remainingDeadline - makeSpan;
            for (Task task: wf.getTaskList()){
                double executionTime = taskExecutionTimes.get(task).get(chosenType)
                        + task.getTransferTime(Parameters.VM_BW);
                task.setSubDeadline( executionTime + (( executionTime / makeSpan) * totalSpareTime));
            }
        }else{
            for (Task task: wf.getTaskList()){
                task.setSubDeadline(taskExecutionTimes.get(task).get(Parameters.VM_TYPES_NUMBERS-1)
                        + task.getTransferTime(Parameters.VM_BW));
            }
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
