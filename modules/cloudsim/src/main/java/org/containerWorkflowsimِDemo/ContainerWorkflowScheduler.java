package org.containerWorkflowsimِDemo;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.containerWorkflowsimِDemo.failure.ContainerFailureGenerator;
import org.containerWorkflowsimِDemo.utils.ContainerParameters;
import org.workflowsim.WorkflowSimTags;
import org.containerWorkflowsimِDemo.scheduling.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainerWorkflowScheduler extends ContainerDatacenterBroker {

    private int workflowEngineId;

    public ContainerWorkflowScheduler(String name, double overBookingfactor) throws Exception{
        super(name, overBookingfactor);
    }
    public ContainerWorkflowScheduler(String name) throws Exception{
        super(name, 1);
    }
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            case containerCloudSimTags.VM_NEW_CREATE:
                processNewVmCreate(ev);
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case containerCloudSimTags.CONTAINER_CREATE_ACK:
                processContainerCreate(ev);
                break;
                //TODO EHSAN CHANGE TAG WITH CONTAINERWorkflowSimTags
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;
            case WorkflowSimTags.CLOUDLET_UPDATE:
                processCloudletUpdate(ev);
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private ContainerBaseSchedulingAlgorithm getScheduler(ContainerParameters.SchedulingAlgorithm name) {
        ContainerBaseSchedulingAlgorithm algorithm;
        switch (name) {
            //by default it is Static
//            case FCFS:
//                algorithm = new FCFSSchedulingAlgorithm();
//                break;
//            case MINMIN:
//                algorithm = new MinMinSchedulingAlgorithm();
//                break;
//            case MAXMIN:
//                algorithm = new MaxMinSchedulingAlgorithm();
//                break;
//            case MCT:
//                algorithm = new MCTSchedulingAlgorithm();
//                break;
//            case DATA:
//                algorithm = new DataAwareSchedulingAlgorithm();
//                break;
            case STATIC:
                algorithm = new ContainerStaticSchedulingAlgorithm();
                break;
//            case ROUNDROBIN:
//                algorithm = new RoundRobinSchedulingAlgorithm();
//                break;
            default:
                algorithm = new ContainerStaticSchedulingAlgorithm();
                break;

        }
        return algorithm;
    }

    //TODO EHSAN CHECL ALL PROCESSING FUNCTIONS FOR compatibility
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            if (ContainerVmList.getById(getVmList(), vmId) != null) {
                getVmsCreatedList().add(ContainerVmList.getById(getVmList(), vmId));
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                        " has been created in Datacenter #", datacenterId, ", Host #",
                        ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId());
                setNumberOfCreatedVMs(getNumberOfCreatedVMs() + 1);
            }
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }
        incrementVmsAcks();

        if(getVmList().size() == vmsAcks){

            submitContainers();
        }
    }
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        ContainerCloudlet cloudlet = (ContainerCloudlet) ev.getData();
        ContainerJob job = (ContainerJob) cloudlet;

        ContainerFailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);

        ContainerCondorVM vm = (ContainerCondorVM) getVmsCreatedList().get(cloudlet.getVmId());
        //so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;

        if (ContainerParameters.getOverheadParams().getPostDelay() != null) {
            delay = ContainerParameters.getOverheadParams().getPostDelay(job);
        }
        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);

    }
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        //int gisID = CloudSim.getEntityId(regionalCisName);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }

    @Override
    public void shutdownEntity() {
        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");
    }

    @Override
    //TODO EHSAN ...
    protected void submitCloudlets() {
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }

    /**
     * A trick here. Assure that we just submit it once
     * TODO EHSAN what is this trick?
     */
    private boolean processCloudletSubmitHasShown = false;

    protected void processCloudletSubmit(SimEvent ev) {
        List<ContainerJob> list = (List) ev.getData();
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }
    protected void processCloudletUpdate(SimEvent ev) {
        ContainerBaseSchedulingAlgorithm scheduler = getScheduler(ContainerParameters.getSchedulingAlgorithm());
        scheduler.setCloudletList(getCloudletList());
        scheduler.setVmList(getVmsCreatedList());
        scheduler.setContainerList(getContainersCreatedList());
        try {
            scheduler.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }
        List<ContainerCloudlet> scheduledList = scheduler.getScheduledList();

        for (ContainerCloudlet cloudlet : scheduledList) {
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if (ContainerParameters.getOverheadParams().getQueueDelay() != null) {
                delay = ContainerParameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();
    }

    @Override
    protected void submitContainers() {
        List<Container> successfullySubmitted = new ArrayList<>();
        int i = 0;
        for(Container container:getContainerList()) {
//            ContainerCloudlet cloudlet = getCloudletList().get(i);
            //Log.printLine("Containers Created" + getContainersCreated());

//            if (cloudlet.getUtilizationModelCpu() instanceof UtilizationModelPlanetLabInMemory) {
//                UtilizationModelPlanetLabInMemory temp = (UtilizationModelPlanetLabInMemory) cloudlet.getUtilizationModelCpu();
//                double[] cloudletUsage = temp.getData();
//                Percentile percentile = new Percentile();
//                double percentileUsage = percentile.evaluate(cloudletUsage, getOverBookingfactor());
//                //Log.printLine("Container Index" + containerIndex);
//                double newmips = percentileUsage * container.getMips();
////                    double newmips = percentileUsage * container.getMips();
////                    double maxUsage = Doubles.max(cloudletUsage);
////                    double newmips = maxUsage * container.getMips();
//                container.setWorkloadMips(newmips);
////                    bindCloudletToContainer(cloudlet.getCloudletId(), container.getId());
//                cloudlet.setContainerId(container.getId());
//                if(cloudlet.getContainerId() != container.getId()){
////                        Log.printConcatLine("Binding Cloudlet: ", cloudlet.getCloudletId(), "To Container: ",container.getId() , "Now it is", cloudlet.getContainerId());
//                }
//
//            }
            i++;

        }

        for(Container container:getContainerList()){
            successfullySubmitted.add(container);

        }
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, successfullySubmitted);
    }

    @Override
    public void processContainerCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        int containerId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            if(vmId ==-1){
                Log.printConcatLine("Error : Where is the VM");}
            else{
                getContainersToVmsMap().put(containerId, vmId);
                getContainersCreatedList().add(ContainerList.getById(getContainerList(), containerId));

//            ContainerVm p= ContainerVmList.getById(getVmsCreatedList(), vmId);
                int hostId = ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId();
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The Container #", containerId,
                        ", is created on Vm #",vmId
                        , ", On Host#", hostId);
                setContainersCreated(getContainersCreated()+1);}
        } else {
            //Container container = ContainerList.getById(getContainerList(), containerId);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }
        incrementContainersAcks();
        if (getContainersAcks() == getContainerList().size()) {
            //Log.print(getContainersCreatedList().size() + "vs asli"+getContainerList().size());
            submitCloudlets();
            getContainerList().clear();
        }
    }

    @Override
    protected void processNewVmCreate(SimEvent ev) {
        Map<String, Object> map = (Map<String, Object>) ev.getData();
        int datacenterId = (int) map.get("datacenterID");
        int result = (int) map.get("result");
        ContainerVm containerVm = (ContainerVm) map.get("vm");
        int vmId = containerVm.getId();
        if (result == CloudSimTags.TRUE) {
            getVmList().add(containerVm);
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(containerVm);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId());
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }
    }

    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
//        setDatacenterIdsList(CloudSim.getCloudResourceList());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());

        //Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloud Resource List received with ",
//                getDatacenterIdsList().size(), " resource(s)");

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }
}
