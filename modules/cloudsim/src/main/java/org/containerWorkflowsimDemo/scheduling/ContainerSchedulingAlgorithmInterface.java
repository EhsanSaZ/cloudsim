package org.containerWorkflowsimDemo.scheduling;

import java.util.List;
//TODO EHSAN : ADD SUPPORT FOR CONTAINER
public interface ContainerSchedulingAlgorithmInterface {

    public void setCloudletList(List list);

    public void setVmList(List list);


    public List getCloudletList();

    public List getVmList();

    public void run() throws Exception;

    public List getScheduledList();
}
