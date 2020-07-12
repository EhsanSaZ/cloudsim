package org.mysim.utils;


import org.mysim.Workflow;

import java.util.List;

public class WorkflowList {
    public static <T extends Workflow> T getById(List<T> workflowList, int id) {
        for (T wf : workflowList) {
            if (wf.getWorkflowId() == id) {
                return wf;
            }
        }
        return null;
    }

}
