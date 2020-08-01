package org.mysim.utils;

import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.Workflow;

import java.util.HashMap;
import java.util.Map;

public class QOSGenerator {
    private Workflow workflow;
    private Map<Task, Double> taskMINExecutionTimes;
    private Map<Task, Double> taskMAXExecutionTimes;
    private Map<Task, Double> taskTransferTimes;
    private Map<Task, Double> rankMakeSpan;
//    private Map<Task, Double> taskOutputTransferTimes;
    private double averageBandwidth;
    private UniformRealDistribution uniformDistribution;

    public QOSGenerator(){
        taskMINExecutionTimes = new HashMap<>();
        taskMAXExecutionTimes = new HashMap<>();
        taskTransferTimes = new HashMap<>();
        rankMakeSpan = new HashMap<>();
//        taskOutputTransferTimes = new HashMap<>();
        averageBandwidth = Parameters.VM_BW;
        uniformDistribution = new UniformRealDistribution();
    }
    public void run(){
        Log.printConcatLine(CloudSim.clock(), ": QOSGenerator: starting calculation for workflow #",
                workflow.getWorkflowId());
        calculateTasksRunningTimes();

        workflow.setDeadline(generateDeadline());
        workflow.setBudget(generateBudget());
        finish();
    }

    private void calculateTasksRunningTimes() {
        for (Task t : workflow.getTaskList()) {
            taskMAXExecutionTimes.put(t,
                    t.getCloudletLength() / (Parameters.VM_MIPS[0] * Parameters.VM_PES[0]) );
            taskMINExecutionTimes.put(t,
                    t.getCloudletLength() / (Parameters.VM_MIPS[Parameters.VM_TYPES_NUMBERS-1] * Parameters.VM_PES[Parameters.VM_TYPES_NUMBERS-1]));
            double transferTime = 0.0;
            for (FileItem file: t.getFileList()){
                if (file.isRealInputFile(t.getFileList()) || file.isRealOutputFile(t.getFileList())){
                    transferTime += file.getSize()  * 8 / (double) Consts.MILLION / averageBandwidth;
                }
            }
            taskTransferTimes.put(t, transferTime);
        }
    }
    public double generateDeadline(){
        Log.printConcatLine(CloudSim.clock(), ": QOS: Generating Deadline for workflow #", workflow.getWorkflowId());
        double minMakeSpan = estimateMinMakeSpan();
        double maxMakeSpan = estimateMaxMakeSpan();

//        return minMakeSpan + (maxMakeSpan - minMakeSpan) * uniformDistribution.sample();
        return minMakeSpan + (maxMakeSpan - minMakeSpan) * Parameters.ALPHA_DEADLINE_FACTOR;
    }
    public double generateBudget(){
        Log.printConcatLine(CloudSim.clock(), ": QOS: Generating Budget for workflow #", workflow.getWorkflowId());
        double minCost = estimateMinCost();
        double maxCost = estimateMaxCost();
//        return minCost + (maxCost - minCost) * uniformDistribution.sample();
        return minCost + (maxCost - minCost) * Parameters.BETA_BUDGET_FACTOR;
    }

    private double estimateMinMakeSpan(){
        for(Task task: workflow.getTaskList()){
            calculateRankMakeSpan(task);
        }
        double minMakeSpan = 0.0;
        for( double time: rankMakeSpan.values()){
            minMakeSpan = Math.max(minMakeSpan, time);
        }

        return minMakeSpan;
    }

    private double calculateRankMakeSpan(Task task) {
        if(rankMakeSpan.containsKey(task)){
            return rankMakeSpan.get(task);
        }
        double taskTotalExecutionTime = taskMINExecutionTimes.get(task) + taskTransferTimes.get(task);
        double max = 0.0;
        for(Task child: task.getChildList()){
            double childExecutionTime = calculateRankMakeSpan(child);
            max = Math.max(max, childExecutionTime);
        }
        rankMakeSpan.put(task, taskTotalExecutionTime + max);

        return rankMakeSpan.get(task);
    }

    private double estimateMaxCost(){
        double maxCost = 0.0;
        // T ODO EHSAN: or do it in a simple way
        // T ODO EHSAN: use newcostmodel...
        for(Task task: taskMINExecutionTimes.keySet()){
            maxCost +=Parameters.COST[Parameters.VM_TYPES_NUMBERS-1] *
                    Math.ceil((taskMINExecutionTimes.get(task) + taskTransferTimes.get(task)) / Parameters.BILLING_PERIOD);
        }
        return maxCost;
    }

    private double estimateMaxMakeSpan(){
        double maxMakeSpan = 0.0;
        //The maximum time was defined as the execution
        //time resulting from running all tasks sequentially on
        //a single VM of the slowest type
        for(Task t : workflow.getTaskList()){
            maxMakeSpan += taskMAXExecutionTimes.get(t) + taskTransferTimes.get(t);
        }
        return maxMakeSpan;
    }

    private double estimateMinCost(){
        double minCost = 0.0;
        // T ODO EHSAN: or do it in a simple way
        // T ODO EHSAN: use newcostmodel...
        for(Task task: taskMAXExecutionTimes.keySet()){
            minCost +=Parameters.COST[0] * Math.ceil((taskMAXExecutionTimes.get(task) + taskTransferTimes.get(task)) / Parameters.BILLING_PERIOD);
        }
        return minCost;
    }
    //---------setter and getter
    public Workflow getWorkflow() {
        return workflow;
    }

    public void setWorkflow(Workflow workflow) {
        this.workflow = workflow;
    }

    public void finish() {
        Log.printConcatLine(CloudSim.clock(), ": QOSGenerator: finish Calculation for workflow #", workflow.getWorkflowId());

        taskMINExecutionTimes.clear();
        taskMAXExecutionTimes.clear();
        taskTransferTimes.clear();
        rankMakeSpan.clear();
//        taskOutputTransferTimes.clear();
        workflow = null;
    }
}
