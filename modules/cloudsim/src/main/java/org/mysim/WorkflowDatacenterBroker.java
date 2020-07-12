package org.mysim;

import org.cloudbus.cloudsim.container.core.ContainerDatacenterBroker;

public class WorkflowDatacenterBroker extends ContainerDatacenterBroker {
    public WorkflowDatacenterBroker(String name, double overBookingfactor) throws Exception {
        super(name, overBookingfactor);
    }


}
