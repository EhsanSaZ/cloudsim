package org.containerWorkflowsimِDemo;

import org.cloudbus.cloudsim.ParameterException;
import org.workflowsim.ClusterStorage;

public class ContainerClusterStorage extends ClusterStorage {

    public ContainerClusterStorage(String name, double capacity) throws ParameterException {
        super(name, capacity);
    }
}
