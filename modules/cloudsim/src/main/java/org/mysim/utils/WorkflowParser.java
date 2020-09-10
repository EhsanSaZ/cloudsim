package org.mysim.utils;

import org.mysim.Task;
import org.mysim.Workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class WorkflowParser {

    public abstract List<String> getWorkflowPathList();
    public abstract void setWorkflowPathList(List<String> workflowPathList);

    public abstract int getJobIdStartsFrom();
    public abstract void setJobIdStartsFrom(int jobIdStartsFrom);
    public abstract int getUserId();
    public abstract boolean hasNextWorkflow();
    public abstract Workflow get_next_workflow();
    public abstract void populateWorkflowPathList();
    public abstract int getTotalWorkflowNumbers();
    public abstract void setTotalWorkflowNumbers(int totalWorkflowNumbers);
}
