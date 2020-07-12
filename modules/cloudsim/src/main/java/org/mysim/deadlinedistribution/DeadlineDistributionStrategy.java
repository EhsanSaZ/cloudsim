package org.mysim.deadlinedistribution;

import org.mysim.Workflow;

public abstract class DeadlineDistributionStrategy {

    Workflow wf;

    public abstract void run();
    public abstract void updateSubDeadlines();

    public Workflow getWorkflow() {
        return wf;
    }

    public void setWorkflow(Workflow workflow) {
        this.wf = workflow;
    }
}
