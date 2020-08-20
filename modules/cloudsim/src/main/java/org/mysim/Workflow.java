package org.mysim;

import java.util.ArrayList;
import java.util.List;

public class Workflow {

    private List<Task> taskList;
    private List<Task> submittedTaskList;
    private List<Task> executedTaskList;

    //unique name
    private String name;

    private final int workflowId;

    private double deadline;

    private double budget;

    private double finalMakeSpan;

    private double totalCost;

    private int taskNumbers;

    public Workflow(String name,final int workflowId, int deadline, int budget){
            setName(name);
            this.workflowId = workflowId;
            setTaskList(new ArrayList<>());
            setSubmittedTaskList(new ArrayList<>());
            setExecutedTaskList(new ArrayList<>());
            setDeadline(deadline);
            setBudget(budget);
            setFinalMakeSpan(-1);
            setTotalCost(0);
            setTaskNumbers(-1);
    }

    public double getCurrentMakeSpan(){
        double maxFinishTime= Double.MIN_VALUE;
        double minStartTime = Double.MAX_VALUE;
        if (getExecutedTaskList().size() > 0){
            for(Task task: getExecutedTaskList()){
                if(task.getFinishTime() >= 0){
                    maxFinishTime = Math.max(maxFinishTime, task.getFinishTime());
                }
                if(task.getExecStartTime() >=0){
                    minStartTime = Math.min(minStartTime, task.getExecStartTime());
                }
            }
            return maxFinishTime - minStartTime;
        }
        if (getFinalMakeSpan() > 0){
            return getFinalMakeSpan();
        }
        return 0.0;
    }

    //---------------------------setter and getter--------------------
    public List<Task> getTaskList() {
        return taskList;
    }

    public void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }

    public List<Task> getSubmittedTaskList() { return submittedTaskList; }

    public void setSubmittedTaskList(List<Task> submittedTaskList) { this.submittedTaskList = submittedTaskList; }

    public List<Task> getExecutedTaskList() { return executedTaskList; }

    public void setExecutedTaskList(List<Task> executedTaskList) { this.executedTaskList = executedTaskList; }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getDeadline() {
        return deadline;
    }

    public void setDeadline(double deadline) {
        this.deadline = deadline;
    }

    public double getBudget() {
        return budget;
    }

    public void setBudget(double budget) {
        this.budget = budget;
    }

    public double getFinalMakeSpan() {
        return finalMakeSpan;
    }

    public void setFinalMakeSpan(double finalMakeSpan) {
        this.finalMakeSpan = finalMakeSpan;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(double totalCost) {
        this.totalCost = totalCost;
    }

    public int getWorkflowId() {
        return workflowId;
    }

    public int getTaskNumbers() {
        return taskNumbers;
    }

    public void setTaskNumbers(int taskNumbers) {
        this.taskNumbers = taskNumbers;
    }
}
