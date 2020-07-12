package org.mysim.budgetdistribution;

import org.mysim.FileItem;
import org.mysim.Task;
import org.mysim.Workflow;
import org.mysim.utils.Parameters;

import java.util.HashMap;
import java.util.Map;

public class BudgetDistributionStrategySpareBudget extends BudgetDistributionStrategy{

    @Override
    public void calculateSubBudget(Workflow wf, Task task) {
        double spentBudget = wf.getTotalCost();
//        for( Task t : wf.getExecutedTaskList()){
//            spentBudget += t.getTaskExecutionCost();
//        }

        Map<Task, Map<Integer, Double>> unallocTaskCostMap = new HashMap<>();
        Map<Task, Double> unallocMinCostMap = new HashMap<>();

        for(Task t : wf.getTaskList()){
            Map<Integer, Double> costMap = new HashMap<>();

            double minCost = Double.MAX_VALUE;
            unallocMinCostMap.put(t, minCost);

            for (int i = 0; i < Parameters.VM_TYPES_NUMBERS; i++){
                double cost =  calculateTaskExecutionCostOnVmType(t, i);
                costMap.put(i, cost);
                if (cost< unallocMinCostMap.get(t)){
                    unallocMinCostMap.put(t, cost);
                }
            }
            unallocTaskCostMap.put(t, costMap);
        }

        double estimatedMinNeededBudget = 0.0;
        for (double c : unallocMinCostMap.values()){
            estimatedMinNeededBudget += c;
        }

        double spareBudget = wf.getBudget() - spentBudget - estimatedMinNeededBudget;

        if(spareBudget > 0){
            double ave_current = 0.0;
            for(double c : unallocTaskCostMap.get(task).values()){
                ave_current += c;
            }
            ave_current /= unallocTaskCostMap.get(task).size();

            double ave_total = 1;

            for (Task t : unallocTaskCostMap.keySet()){
                double ave_t = 0.0;
                for(double c : unallocTaskCostMap.get(t).values()){
                    ave_t += c;
                }
                ave_t /= unallocTaskCostMap.get(t).size();
                ave_total += ave_t;
            }
            task.setSubBudget(unallocMinCostMap.get(task) + spareBudget * (ave_current / ave_total));
        }else {
            task.setSubBudget(unallocMinCostMap.get(task));
        }

        unallocTaskCostMap.clear();
        unallocMinCostMap.clear();
    }

    private double calculateTaskExecutionCostOnVmType(Task task, int vmTypeIndex){
        //TODO EHSAN: this estimation must be implemented
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
}
