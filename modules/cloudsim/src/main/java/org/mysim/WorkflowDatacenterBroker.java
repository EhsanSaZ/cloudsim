package org.mysim;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
;
import org.mysim.utils.MySimTags;
import org.mysim.utils.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkflowDatacenterBroker extends ContainerDatacenterBroker {

    protected Map<Integer,List<Integer>> vmToDatacenterRequestedIdsList;
    private int workflowEngineId;

    public WorkflowDatacenterBroker(String name, double overBookingfactor, int workflowEngineId) throws Exception {
        super(name, overBookingfactor);
        setVmToDatacenterRequestedIdsList(new HashMap<Integer, List<Integer>>());
        setWorkflowEngineId(workflowEngineId);
        setVmsAcks(0);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // A finished cloudlet returned
            case MySimTags.CLOUDLET_CHECK:
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            case MySimTags.VM_CREATE_DYNAMIC_ACK:
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreateDynamic(ev);
                break;
            case containerCloudSimTags.CONTAINER_CREATE_ACK:
            case MySimTags.CONTAINER_SUBMIT_DYNAMIC_ACK:
                processContainerCreateDynamic(ev);
                break;
//            case MySimTags.CLOUDLET_SUBMIT_DYNAMIC:
//                processCloudletSubmitDynamic(ev);
//                break;
//            case MySimTags.CLOUDLET_UPDATE:
//                processCloudletUpdate(ev);
//                break;

            // it is probably not needed at all from here to.. new signals are used
            // VM Creation answer
//            case CloudSimTags.VM_CREATE_ACK:
//                processVmCreate(ev);
//                break;
            // New VM Creation answer
            case containerCloudSimTags.VM_NEW_CREATE:
                processNewVmCreate(ev);
                break;
//            case containerCloudSimTags.CONTAINER_CREATE_ACK:
//                processContainerCreate(ev);
//                break;
            //..it is probably not needed at all to here.. new signals are used
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    // no change same as parent class
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        setDatacenterIdsList(CloudSim.getCloudResourceList());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }

    @Override
    protected void processResourceCharacteristics(SimEvent ev) {
        ContainerDatacenterCharacteristics characteristics = (ContainerDatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
            getDatacenterCharacteristicsList().clear();
            setDatacenterRequestedIdsList(new ArrayList<Integer>());
            //createVmsInDatacenter(getDatacenterIdsList().get(0));
        }
    }
    protected void processVmCreateDynamic(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(ContainerVmList.getById(getVmList(), vmId));
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            //send response to workflow engine..
            schedule(getWorkflowEngineId(),CloudSim.getMinTimeBetweenEvents(),CloudSimTags.VM_CREATE_ACK,ev);
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }
        incrementVmsAcks();

        // not all the requested VMs have been created
        if (!(getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed())) {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                Boolean all_queried = true;
                for (int vmID : getVmToDatacenterRequestedIdsList().keySet()){
                    if (!(getVmToDatacenterRequestedIdsList().get(vmID).size() == getDatacenterIdsList().size())){
                        all_queried = false;
                        break;
                    }
                }
                // create not created vms in next data center which has not been tried
                if (!all_queried){
                    createVMsInDataCenterDynamic(getVmList());
                    return;
                }
                // all datacenters already queried
                if (!(getVmsCreatedList().size() > 0)) { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }
    protected void processContainerCreateDynamic(SimEvent ev) {
        // weak logic for creation and process response of container create
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

                int hostId = ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId();
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The Container #", containerId,
                        ", is created on Vm #",vmId
                        , ", On Host#", hostId);
                setContainersCreated(getContainersCreated()+1);
                // send response to workflow engine
                schedule(getWorkflowEngineId(),CloudSim.getMinTimeBetweenEvents(), containerCloudSimTags.CONTAINER_CREATE_ACK, ev);
            }
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }
        incrementContainersAcks();
//        if (getContainersAcks() == getContainerList().size()) {
//            //Log.print(getContainersCreatedList().size() + "vs asli"+getContainerList().size());
//            submitCloudlets();
//            getContainerList().clear();
//        }
    }
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        ContainerCloudlet cloudlet = (ContainerCloudlet) ev.getData();
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloudlet ", cloudlet.getCloudletId(),
                " returned");

//        CondorVM vm =  (CondorVM) ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId());
        //vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        //set the workload of this container to 0 so in planning it may be used again with a new task on it else it will be destroyed
        Container container = (Container) ContainerList.getById(getContainersCreatedList(), cloudlet.getContainerId());
        container.setWorkloadMips(0);


//        double delay = 0.0;
//        if (ContainerParameters.getOverheadParams().getPostDelay() != null) {
//            delay = ContainerParameters.getOverheadParams().getPostDelay(job);
//        }
        schedule(getWorkflowEngineId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.CLOUDLET_RETURN, cloudlet);

//        schedule(this.getId(), 0.0, MySimTags.CLOUDLET_UPDATE);
    }

    protected void processCloudletSubmitDynamic(SimEvent ev) { }

    protected void processCloudletUpdate(SimEvent ev) {
        // TODO IMPLEMENT THIS...
    }

    protected void finishExecution() {
        sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
    }

    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
    }

    @Override
    public void shutdownEntity() { Log.printConcatLine(getName(), " is shutting down..."); }

    public void submitTaskListDynamic(List<? extends ContainerCloudlet> taskList){
        // T ODO EHSAN: double check logic
        getCloudletList().addAll(taskList);

        for (ContainerCloudlet cloudlet: taskList){
            if(cloudlet.getContainerId() > 0
                && cloudlet.getVmId() > 0
                && cloudlet.getVmId() == getContainersToVmsMap().get(cloudlet.getContainerId())){
                if(getContainersToVmsMap().get(cloudlet.getContainerId()) != null
                    && getVmsToDatacentersMap().get(cloudlet.getVmId()) != null){
                    //TODO EHSAN: Add delay feature in version 2
//                    int vmId = cloudlet.getVmId();
//                    if (ContainerParameters.getOverheadParams().getQueueDelay() != null) {
//                        delay = ContainerParameters.getOverheadParams().getQueueDelay(cloudlet);
//                    }
//                    schedule(getVmsToDatacentersMap().get(cloudlet.getVmId()), CloudSim.getMinTimeBetweenEvents(),
//                            MySimTags.CLOUDLET_SUBMIT_DYNAMIC, cloudlet);
                    schedule(getVmsToDatacentersMap().get(cloudlet.getVmId()), CloudSim.getMinTimeBetweenEvents(),
                            CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                }
            }
        }
    }

    public void submitContainerListDynamic(List<? extends Container> containerList){
        // T ODO EHSAN: double check logic
        // weak logic for creation and process response of container create
        //int requestedContainers = getContainer
        getContainerList().addAll( containerList);
        List <Container> successfullySubmitted = new ArrayList<>();

        //successfullySubmitted.addAll(containerList);
        for(Container c: containerList){
            if (!getContainersToVmsMap().containsKey(c.getId())){
                successfullySubmitted.add(c);
            }
        }

//        sendNow(getDatacenterIdsList().get(0), MySimTags.CONTAINER_SUBMIT_DYNAMIC_ACK, successfullySubmitted);
//        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, successfullySubmitted);
        schedule(getDatacenterIdsList().get(0), Parameters.CONTAINER_PROVISIONING_DELAY, containerCloudSimTags.CONTAINER_SUBMIT, successfullySubmitted);
    }
     public void createVMsInDataCenterDynamic (List< ? extends ContainerVm> vmList){
         // T ODO EHSAN: double check logic
         for (ContainerVm vm: vmList){
             if (!getVmList().contains(vm)){
                 getVmList().add(vm);
             }
         }

         int requestedVms = getVmsRequested();
         for(ContainerVm vm: getVmList()){
             if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
                 if (!getVmToDatacenterRequestedIdsList().containsKey(vm.getId())){
                     int nextDataCenterId = getDatacenterIdsList().get(0);
                     List <Integer> requestedList = new ArrayList<>();
                     requestedList.add(nextDataCenterId);
                     getVmToDatacenterRequestedIdsList().put(vm.getId(), requestedList);
                     String nextDataCenterName = CloudSim.getEntityName(nextDataCenterId);
                     Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
                             + " in " + nextDataCenterName + " DYNAMIC TOR");
//                     sendNow(nextDataCenterId, MySimTags.VM_CREATE_DYNAMIC_ACK, vm);
//                     sendNow(nextDataCenterId, CloudSimTags.VM_CREATE_ACK, vm);
                     schedule(nextDataCenterId, Parameters.VM_PROVISIONING_DELAY,CloudSimTags.VM_CREATE_ACK, vm);
                     requestedVms++;
                 }else{
                     List <Integer> requestedList = getVmToDatacenterRequestedIdsList().get(vm.getId());
                     for (int nextDataCenterId : getDatacenterIdsList()) {
                         if (!requestedList.contains(nextDataCenterId)){
                             requestedList.add(nextDataCenterId);
                             getVmToDatacenterRequestedIdsList().put(vm.getId(), requestedList);
                             String nextDataCenterName = CloudSim.getEntityName(nextDataCenterId);
                             Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
                                     + " in " + nextDataCenterName + " DYNAMIC TOR");
//                             sendNow(nextDataCenterId, MySimTags.VM_CREATE_DYNAMIC_ACK, vm);
//                             sendNow(nextDataCenterId, CloudSimTags.VM_CREATE_ACK, vm);
                             schedule(nextDataCenterId, Parameters.VM_PROVISIONING_DELAY, CloudSimTags.VM_CREATE_ACK, vm);
                             requestedVms++;
                         }
                     }
                 }
             }
         }
         setVmsRequested(requestedVms);
         // T ODO not sure about this
//        setVmsAcks(0);
     }

     public void destroyContainer(Container container){
        // must use this only when there is no task running on the container...
         if(getContainersCreatedList().contains(container)){
             getContainersCreatedList().remove(container);
             getContainersToVmsMap().remove(container.getId());
         }
         int datacenterId = getVmsToDatacentersMap().get(container.getVm().getId());
         schedule(datacenterId, Parameters.CONTAINER_DESTROY_DELAY, MySimTags.CONTAINER_DESTROY, container);
         getContainerList().remove(container);

     }

    public void destroyVms(List<? extends ContainerVm> list){
        // use this only when there are no containers on the vm running any tasks
        // T ODO EHSAN: this logic is not true..
        // weak logic for creation and process response of container create
        for(ContainerVm vm : list){
            int datacenterId = getVmsToDatacentersMap().get(vm.getId());
//            List <Container> toRemove = new ArrayList<>();
//            for (Container c: getContainersCreatedList()){
//                if (c.getVm().getId()==vm.getId()){
//                    toRemove.add(c);
//                    getContainersToVmsMap().remove(c.getId());
//                }
//            }
//            getContainersCreatedList().removeAll(toRemove);
//            getContainerList().removeAll(toRemove);

            // some tasks are scheduled on com containers on this vm for future and yet this vm is going to be destroyed.
            // we should be aware for this for any logic.. below or up
            //actually there must be no container on this vm if we are going to destroy it
            // and we must to migrate those containers on this vm before destroying
            if (vm.getContainerList().size() > 0) {
                for (Container container: vm.getContainerList()){
                    if(getContainersCreatedList().contains(container)){
//                    toRemove.add(c);
                        //remove all containers on this vm  from container created list and the map
                        getContainersCreatedList().remove(container);
                        getContainersToVmsMap().remove(container.getId());
                    }
                    schedule(datacenterId, Parameters.CONTAINER_DESTROY_DELAY , MySimTags.CONTAINER_DESTROY, container);
                    getContainerList().remove(container);
                }
            }
            //Destroy all containers on this vm
//            vm.containerDestroyAll();

            getVmsToDatacentersMap().remove(vm.getId());
            getVmsCreatedList().remove(vm);

            setVmsDestroyed(getVmsDestroyed() +1);

            schedule(datacenterId, Parameters.VM_DESTROY_DELAY, CloudSimTags.VM_DESTROY, vm);
        }
    }

    ////////////////////setter and getter
    protected void setVmToDatacenterRequestedIdsList(Map<Integer,List<Integer>> datacenterRequestedIdsListMap){
        this.vmToDatacenterRequestedIdsList = datacenterRequestedIdsListMap;
    }

    protected Map<Integer,List<Integer>> getVmToDatacenterRequestedIdsList(){
        return vmToDatacenterRequestedIdsList;
    }

    public int getWorkflowEngineId() {
        return workflowEngineId;
    }

    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }
}
