package org.mysim.budgetdistribution;

import org.mysim.Task;
import org.mysim.Workflow;

public abstract class BudgetDistributionStrategy {
    public abstract void calculateSubBudget(Workflow wf, Task task);
    public abstract void calculateSubBudgetWholeWorkflow(Workflow wf);
}
