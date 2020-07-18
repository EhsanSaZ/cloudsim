package org.mysim;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import org.mysim.utils.MySimTags;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;


import java.util.Iterator;
import java.util.List;

public class WorkflowContainerDatacenter extends ContainerDatacenter {

//    public WorkflowPowerContainerDatacenterCM(String name,
//                                              ContainerDatacenterCharacteristics characteristics,
//                                              ContainerVmAllocationPolicy vmAllocationPolicy,
//                                              ContainerAllocationPolicy containerAllocationPolicy,
//                                              List<Storage> storageList,
//                                              double schedulingInterval, String experimentName, String logAddress,
//                                              double vmStartupDelay, double containerStartupDelay) throws Exception {
//        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList,
//                schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
//    }

    public WorkflowContainerDatacenter(String name,
                                       ContainerDatacenterCharacteristics characteristics,
                                       ContainerVmAllocationPolicy vmAllocationPolicy,
                                       ContainerAllocationPolicy containerAllocationPolicy,
                                       List<Storage> storageList,
                                       double schedulingInterval, String experimentName, String logAddress) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList,
                schedulingInterval, experimentName, logAddress);
    }

    @Override
    public void processEvent(SimEvent ev) {
        int srcId = -1;

        switch (ev.getTag()) {
            // Resource characteristics inquiry
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), getCharacteristics());
                break;

            // Resource dynamic info inquiry
            case CloudSimTags.RESOURCE_DYNAMICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), 0);
                break;

            case CloudSimTags.RESOURCE_NUM_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int numPE = getCharacteristics().getNumberOfPes();
                sendNow(srcId, ev.getTag(), numPE);
                break;

            case CloudSimTags.RESOURCE_NUM_FREE_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int freePesNumber = getCharacteristics().getNumberOfFreePes();
                sendNow(srcId, ev.getTag(), freePesNumber);
                break;

            // New Cloudlet arrives
            // TODO: remove new tag in broker.. old ones are ok..
            case  MySimTags.CLOUDLET_SUBMIT_DYNAMIC:
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev, false);
                break;

            // New Cloudlet arrives, but the sender asks for an ack
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processCloudletSubmit(ev, true);
                break;

            // Cancels a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudlet(ev, CloudSimTags.CLOUDLET_CANCEL);
                break;

            // Pauses a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE);
                break;

            // Pauses a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE_ACK);
                break;

            // Resumes a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_RESUME:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME);
                break;

            // Resumes a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME_ACK);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE_ACK:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE_ACK);
                break;

            // Checks the status of a Cloudlet
            case CloudSimTags.CLOUDLET_STATUS:
                processCloudletStatus(ev);
                break;

            // Ping packet
            case CloudSimTags.INFOPKT_SUBMIT:
                processPingRequest(ev);
                break;

            case CloudSimTags.VM_CREATE:
                processVmCreate(ev, false);
                break;
            // TODO: remove new tag in broker.. old ones are ok..
            case  MySimTags.VM_CREATE_DYNAMIC_ACK:
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev, true);
                break;

            case CloudSimTags.VM_DESTROY:
                processVmDestroy(ev, false);
                break;

            case CloudSimTags.VM_DESTROY_ACK:
                processVmDestroy(ev, true);
                break;

            case CloudSimTags.VM_MIGRATE:
                processVmMigrate(ev, false);
                break;

            case CloudSimTags.VM_MIGRATE_ACK:
                processVmMigrate(ev, true);
                break;

            case CloudSimTags.VM_DATA_ADD:
                processDataAdd(ev, false);
                break;

            case CloudSimTags.VM_DATA_ADD_ACK:
                processDataAdd(ev, true);
                break;

            case CloudSimTags.VM_DATA_DEL:
                processDataDelete(ev, false);
                break;

            case CloudSimTags.VM_DATA_DEL_ACK:
                processDataDelete(ev, true);
                break;

            case CloudSimTags.VM_DATACENTER_EVENT:
                updateCloudletProcessing();
                checkCloudletCompletion();
                break;

            case MySimTags.CONTAINER_SUBMIT_DYNAMIC_ACK:
            case containerCloudSimTags.CONTAINER_SUBMIT:
                processContainerSubmit(ev, true);
                break;

            case containerCloudSimTags.CONTAINER_MIGRATE:
                processContainerMigrate(ev, false);
                // other unknown tags are processed by this method
                break;

            case MySimTags.CONTAINER_DESTROY:
                processContainerDestroy(ev, false);
                break;
            case MySimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev, true);
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();

        /**
         * cl is actually a task but it is not necessary to cast it to a job
         */
        try {
            Task task = (Task) ev.getData();
            if (task.isFinished()){
                String name = CloudSim.getEntityName(task.getUserId());
                Log.printLine(getName() + ": Warning - Cloudlet #" + task.getCloudletId() + " owned by " + name
                        + " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();
                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = task.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(task.getUserId(), tag, data);
                }
                return;
            }
            int userId = task.getUserId();
            int vmId = task.getVmId();
            int containerId = task.getContainerId();

            ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
            CondorVM vm = (CondorVM) host.getContainerVm(vmId, userId);
            Container container = vm.getContainer(containerId, userId);

            //T ODO EHSAN consider container in cost model
            switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    task.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    task.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }

            /**
             * Stage-in file && Shared based on the file.system
             */
            if (task.getClassType() == Parameters.ClassType.STAGE_IN.value) {
                stageInFile2FileSystem(task);
            }
            /**
             * Add data transfer time (communication cost
             */
            double input_fileTransferTime = 0.0;
            if (task.getClassType() == Parameters.ClassType.COMPUTE.value) {
                input_fileTransferTime = processDataStageInForComputeJob(task.getFileList(), task);
            }

            double output_fileTransferTime = 0.0;
            if (task.getClassType() == Parameters.ClassType.COMPUTE.value) {
                output_fileTransferTime = processDataStageOutForComputeJob(task.getFileList(), task);
            }
            double totalTransterTime = input_fileTransferTime + output_fileTransferTime;

            ContainerCloudletScheduler scheduler = container.getContainerCloudletScheduler();
            //  TODO Ehsan: consider input output transfer time for task finish time.

            double estimatedFinishTime = scheduler.cloudletSubmit(task, totalTransterTime);
//            updateTaskExecTime(job, container);
            task.setTaskFinishTime(task.getExecStartTime() + task.getCloudletLength() / container.getMips());

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                // TODO EHSAN: in original version estimated finish time is sum with transfer time.
                //estimatedFinishTime += input_fileTransferTime
                //estimatedFinishTime += output_fileTransferTime
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM or container");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = task.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(task.getUserId(), tag, data);
            }
        }catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }
    }
    private void stageInFile2FileSystem(Task task){
        List<FileItem> fList = task.getFileList();
        for (FileItem file : fList){
            switch (ReplicaCatalog.getFileSystem()){
                case LOCAL:
                case SHARED:
                    ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    break;
                default:
                    break;
            }

        }
    }
    protected double processDataStageOutForComputeJob(List<FileItem> requiredFiles, Task task) throws Exception {
        double time = 0.0;
        for ( FileItem file: requiredFiles){
            //The input file is not an output File
            if (file.isRealOutputFile(requiredFiles)) {
                //this file will be register on shared and local storage in future for now we just calculate transfer time
                double maxRate = Double.MIN_VALUE;
                for (Storage storage : getStorageList()) {
                    double rate = storage.getMaxTransferRate();
                    if (rate > maxRate) {
                        maxRate = rate;
                    }
                    //Storage storage = getStorageList().get(0);
                }
                // convert B to Mb
                time += file.getSize()  * 8 / (double) Consts.MILLION / maxRate;
            }
        }
        return time;
    }
    protected double processDataStageInForComputeJob(List<FileItem> requiredFiles, Task task) throws Exception {
        double time = 0.0;
        for ( FileItem file: requiredFiles){
            //The input file is not an output File
            if (file.isRealInputFile(requiredFiles)) {
                double maxBwth = 0.0;
                List siteList = ReplicaCatalog.getStorageList(file.getName());
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
                // check if file exists on the vm which the task is running..
                boolean foundOnLocal = false;

                int vmId = task.getVmId();
//                int userId = task.getUserId();
//                ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
//                ContainerVm vm = host.getContainerVm(vmId, userId);
                for (Iterator it = siteList.iterator(); it.hasNext(); ) {
                    //site is where one replica of this data is located at
                    String site = (String) it.next();
                    if (site.equals(this.getName())) {
                        continue;
                    }

                    /**
                     * This file is already in the local vm and thus it
                     * is no need to transfer
                     */
                    if (site.equals(Integer.toString(vmId))) {
                        foundOnLocal = true;
                        break;
                    }

                }
                /**
                 * For the case when storage is too small it is not
                 * handled here
                 */
                //We should add but since CondorVm has a small capability it often fails
                //We currently don't use this storage to do anything meaningful. It is left for future.
                //condorVm.addLocalFile(file);

                 //the file is not on the vm, get from shared storage on data center
                if (!foundOnLocal){
                    double maxRate = Double.MIN_VALUE;
                    for (Storage storage : getStorageList()) {
                        double rate = storage.getMaxTransferRate();
                        if (rate > maxRate) {
                            maxRate = rate;
                        }
                    }
                    //Storage storage = getStorageList().get(0);
                    // file size in is B convert it to Mb because bw is Mbs
                    time += file.getSize()  * 8 / (double) Consts.MILLION / maxRate;
                }
                // because the task is running on the vm for future the vm must contain this file
                ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
//                switch (ReplicaCatalog.getFileSystem()){
//                    case SHARED:
//                        double maxRate = Double.MIN_VALUE;
//                        for (Storage storage : getStorageList()) {
//                            double rate = storage.getMaxTransferRate();
//                            if (rate > maxRate) {
//                                maxRate = rate;
//                            }
//                            //Storage storage = getStorageList().get(0);
//                            time += file.getSize()  * 8 / (double) Consts.MILLION / maxRate;
//                        }
//                        break;
//                case LOCAL:
//                    int vmId = task.getVmId();
//                    int userId = task.getUserId();
//                    ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
//                    ContainerVm vm = host.getContainerVm(vmId, userId);
//
//                    boolean requiredFileStagein = true;
//                    for (Iterator it = siteList.iterator(); it.hasNext(); ) {
//                        //site is where one replica of this data is located at
//                        String site = (String) it.next();
//                        if (site.equals(this.getName())) {
//                            continue;
//                        }
//
//                        /**
//                         * This file is already in the local vm and thus it
//                         * is no need to transfer
//                         */
//                        if (site.equals(Integer.toString(vmId))) {
//                            requiredFileStagein = false;
//                            break;
//                        }
//
//                        double bwth;
//                        if (site.equals(ContainerParameters.SOURCE)) {
//                            //transfers from the source to the VM is limited to the VM bw only
//                            bwth = vm.getBw();
//                            //bwth = dcStorage.getBaseBandwidth();
//                        }else {
//                            //transfers between two VMs is limited to both VMs
//                            bwth = Math.min(vm.getBw(), getVmAllocationPolicy().getHost(Integer.parseInt(site), userId).getContainerVm(Integer.parseInt(site), userId).getBw());
//                            //bwth = dcStorage.getBandwidth(Integer.parseInt(site), vmId);
//                        }
//                        if (bwth > maxBwth) {
//                            maxBwth = bwth;
//                        }
//                    }
//
//                    if (requiredFileStagein && maxBwth > 0.0) {
//                        time += file.getSize() * 8/ (double) Consts.MILLION / maxBwth;
//                    }
//                    /**
//                     * For the case when storage is too small it is not
//                     * handled here
//                     */
//                    //We should add but since CondorVm has a small capability it often fails
//                    //We currently don't use this storage to do anything meaningful. It is left for future.
//                    //condorVm.addLocalFile(file);
//                    ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
//                    break;
//                }
            }
        }
        return time;
    }

    @Override
    protected void processVmCreate(SimEvent ev, boolean ack) {
        ContainerVm containerVm = (ContainerVm) ev.getData();

        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
//            send(containerVm.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.VM_CREATE_ACK, data);
            send(containerVm.getUserId(), CloudSim.getMinTimeBetweenEvents(), MySimTags.VM_CREATE_DYNAMIC_ACK, data);
        }

        if (result) {
            getContainerVmList().add(containerVm);

            if (containerVm.isBeingInstantiated()) {
                containerVm.setBeingInstantiated(false);
            }

            containerVm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(containerVm).getContainerVmScheduler()
                    .getAllocatedMipsForContainerVm(containerVm));
        }
    }

    private void updateTaskExecTime(Task task, Container container) {
        task.setTaskFinishTime(task.getExecStartTime() + task.getCloudletLength() / container.getMips());
    }

    protected void processContainerDestroy(SimEvent ev, boolean ack){
        Container container = (Container) ev.getData();
        // TODO EHSAN: deallocate container for tasks on this container...
        getContainerAllocationPolicy().deallocateVmForContainer(container);
        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = container.getId();
            data[2] = CloudSimTags.TRUE;

            sendNow(container.getUserId(), MySimTags.CONTAINER_DESTROY_ACK, data);
        }
        getContainerList().remove(container);

    }

    @Override
    protected void processVmDestroy(SimEvent ev, boolean ack) {
        ContainerVm containerVm = (ContainerVm) ev.getData();
        // for destroying a vm , first we must remove all containers on it, or migrate them here we remove..
        for( Container container: containerVm.getContainerList()){
            getContainerAllocationPolicy().deallocateVmForContainer(container);
            getContainerList().remove(container);
        }

        getVmAllocationPolicy().deallocateHostForVm(containerVm);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();
            data[2] = CloudSimTags.TRUE;

            sendNow(containerVm.getUserId(), CloudSimTags.VM_DESTROY_ACK, data);
        }

        getContainerVmList().remove(containerVm);
    }

    private void register(Cloudlet cl) {
        Task tl = (Task) cl;
        List<FileItem> fList = tl.getFileList();
        for (FileItem file : fList) {
            if (file.getType() == Parameters.FileType.OUTPUT){ //output file
                ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                int vmId = cl.getVmId();
            /**
             * * Left here for future work
             */
//                int userId = cl.getUserId();
//                ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
//                CondorVM vm = (CondorVM) host.getContainerVm(vmId, userId);
                ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));

            }
        }
    }

}
