package org.mysim.budgetdistribution;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.Workflow;
import org.mysim.utils.Parameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BudgetDistributionStrategySpareBudget extends BudgetDistributionStrategy{

    private  Map<Task, Map<Integer, Double>> unallocTaskCostMap = new HashMap<>();
    private  Map<Task, Double> unallocMinCostMap = new HashMap<>();
    private Map<Task, Double> averageCostMap = new HashMap<>();

    @Override
    public void calculateSubBudgetWholeWorkflow(Workflow wf){
        Log.printConcatLine(CloudSim.clock(),": BudgetDistributor: Start BudgetDistribution for workflow ", wf.getName(),
                " and with ", wf.getTaskList().size() , " tasks.");

        populateCostMaps(wf);

        double estimatedMinNeededBudget = 0.0;
        for (double c : unallocMinCostMap.values()){
            estimatedMinNeededBudget += c;
        }

        double spentBudget = wf.getTotalCost();
        double spareBudget = wf.getBudget() - spentBudget - estimatedMinNeededBudget;

        if(spareBudget > 0){
            double sum_ave_total = 0;
            for( double ave: averageCostMap.values()){
                sum_ave_total += ave;
            }
            for (Task t: wf.getTaskList()){
                double ave_current = averageCostMap.get(t);
                t.setSubBudget(unallocMinCostMap.get(t) + spareBudget * (ave_current / sum_ave_total));
            }
        }else{
            for(Task t : wf.getTaskList()){
                t.setSubBudget(unallocMinCostMap.get(t));
            }
        }
        finish();
        Log.printConcatLine(CloudSim.clock(), ": BudgetDistributor: Budget distribution for workflow ", wf.getName(), " finished");
    }

    @Override
    public void calculateSubBudget(Workflow wf, Task task) {
//        for( Task t : wf.getExecutedTaskList()){
//            spentBudget += t.getTaskExecutionCost();
//        }

//        Map<Task, Map<Integer, Double>> unallocTaskCostMap = new HashMap<>();
//        Map<Task, Double> unallocMinCostMap = new HashMap<>();

//        for(Task t : wf.getTaskList()){
//            Map<Integer, Double> costMap = new HashMap<>();
//
//            double minCost = Double.MAX_VALUE;
//            unallocMinCostMap.put(t, minCost);
//
//            for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
//                double cost =  calculateTaskExecutionCostOnVmType(t, i);
//                costMap.put(i, cost);
//                if (cost< unallocMinCostMap.get(t)){
//                    unallocMinCostMap.put(t, cost);
//                }
//            }
//            unallocTaskCostMap.put(t, costMap);
//        }
        populateCostMaps(wf);

        double estimatedMinNeededBudget = 0.0;
        for (double c : unallocMinCostMap.values()){
            estimatedMinNeededBudget += c;
        }

        double spentBudget = wf.getTotalCost();
        double spareBudget = wf.getBudget() - spentBudget - estimatedMinNeededBudget;

        if(spareBudget > 0){
            double ave_current = averageCostMap.get(task);
//            for(double c : unallocTaskCostMap.get(task).values()){
//                ave_current += c;
//            }
//            ave_current /= unallocTaskCostMap.get(task).size();

            double sum_ave_total = 0;
            for( double ave: averageCostMap.values()){
                sum_ave_total += ave;
            }

            task.setSubBudget(unallocMinCostMap.get(task) + spareBudget * (ave_current / sum_ave_total));
        }else {
            task.setSubBudget(unallocMinCostMap.get(task));
        }

        finish();
    }

    public void populateCostMaps(Workflow wf){
        finish();
        for(Task t : wf.getTaskList()){
            Map<Integer, Double> costMap = new HashMap<>();

            double minCost = Double.MAX_VALUE;
            unallocMinCostMap.put(t, minCost);
            double ave_t = 0.0;
            for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
                double cost =  calculateTaskExecutionCostOnVmType(t, i);
                costMap.put(i, cost);
                ave_t += cost;
                if (cost< unallocMinCostMap.get(t)){
                    unallocMinCostMap.put(t, cost);
                }
            }
            ave_t /= costMap.size();
            unallocTaskCostMap.put(t, costMap);
            averageCostMap.put(t, ave_t);
        }

    }

    private double calculateTaskExecutionCostOnVmType(Task task, int vmTypeIndex){
        //T ODO EHSAN: this estimation must be implemented
        double in_size = 0.0;
        double out_size = 0.0;
        for (FileItem file : task.getFileList()){
            if (file.getType() == Parameters.FileType.INPUT){
                in_size += file.getSize();
            }else {
                out_size += file.getSize();
            }
        }
        double exe_time = (task.getCloudletLength()/(Parameters.VM_MIPS[vmTypeIndex] * Parameters.VM_PES[vmTypeIndex]) +
                in_size/ Parameters.VM_BW + out_size/Parameters.VM_BW);
        return exe_time * Parameters.COST[vmTypeIndex];
    }

    public void finish(){
        unallocTaskCostMap.clear();
        unallocMinCostMap.clear();
        averageCostMap.clear();
    }
}
