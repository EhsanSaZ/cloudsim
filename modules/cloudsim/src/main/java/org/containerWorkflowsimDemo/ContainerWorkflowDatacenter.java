package org.containerWorkflowsimDemo;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.containerWorkflowsimDemo.utils.ContainerParameters;
import org.containerWorkflowsimDemo.utils.ContainerReplicaCatalog;

import java.util.Iterator;
import java.util.List;

public class ContainerWorkflowDatacenter extends ContainerDatacenter {
    public ContainerWorkflowDatacenter(String name, ContainerDatacenterCharacteristics characteristics, ContainerVmAllocationPolicy vmAllocationPolicy,
                                       ContainerAllocationPolicy containerAllocationPolicy, List<Storage> storageList, double schedulingInterval,
                                       String experimentName, String logAddress) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress);

    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();
        /**
         * cl is actually a job but it is not necessary to cast it to a job
         */
        try {
            ContainerJob job = (ContainerJob) ev.getData();
            if (job.isFinished()) {
                String name = CloudSim.getEntityName(job.getUserId());
                Log.printLine(getName() + ": Warning - Cloudlet #" + job.getCloudletId() + " owned by " + name
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
                    data[1] = job.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(job.getUserId(), tag, data);
                }
                return;
            }
            int userId = job.getUserId();
            int vmId = job.getVmId();
            int containerId = job.getContainerId();
            ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
            ContainerCondorVM vm = (ContainerCondorVM) host.getContainerVm(vmId, userId);
            Container container = vm.getContainer(containerId, userId);

            //TODO EHSAN consider container in cost model
            switch (ContainerParameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    job.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    job.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }

            /**
             * Stage-in file && Shared based on the file.system
             */
            if (job.getClassType() == ContainerParameters.ClassType.STAGE_IN.value) {
                stageInFile2FileSystem(job);
            }

            /**
             * Add data transfer time (communication cost
             */
            double fileTransferTime = 0.0;
            if (job.getClassType() == ContainerParameters.ClassType.COMPUTE.value) {
                fileTransferTime = processDataStageInForComputeJob(job.getFileList(), job);
            }

            //CloudletScheduler scheduler = vm.getCloudletScheduler();
            ContainerCloudletScheduler scheduler = container.getContainerCloudletScheduler();
            double estimatedFinishTime = scheduler.cloudletSubmit(job, fileTransferTime);
            updateTaskExecTime(job, container);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM or container");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = job.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(job.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }
        checkCloudletCompletion();

    }

    private void updateTaskExecTime(ContainerJob job, Container container) {
        double start_time = job.getExecStartTime();
        for (ContainerTask task : job.getContainerTaskList()) {
            task.setExecStartTime(start_time);
            double task_runtime = task.getCloudletLength() / container.getMips();
            start_time += task_runtime;
            //Because CloudSim would not let us update end time here
            task.setTaskFinishTime(start_time);
        }
    }

    private void stageInFile2FileSystem(ContainerJob job) {
        List<ContainerFileItem> fList = job.getFileList();
        //TODO EHSAN SHARED IS NOT CORRECT....
        for (ContainerFileItem file : fList) {
            switch (ContainerReplicaCatalog.getFileSystem()) {
                /**
                 * For local file system, add it to local storage (data center
                 * name)
                 */
                case LOCAL:
                    ContainerReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    /**
                     * Is it not really needed currently but it is left for
                     * future usage
                     */
                    //ClusterStorage storage = (ClusterStorage) getStorageList().get(0);
                    //storage.addFile(file);
                    break;
                /**
                 * For shared file system, add it to the shared storage
                 */
                case SHARED:
                    ContainerReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    break;
                default:
                    break;
            }
        }
    }

    protected double processDataStageInForComputeJob(List<ContainerFileItem> requiredFiles, ContainerJob job) throws Exception {
        double time = 0.0;
        // TODO EHSAN consider container BW in calculation too
        for (ContainerFileItem file : requiredFiles) {
            //The input file is not an output File
            if (file.isRealInputFile(requiredFiles)) {
                double maxBwth = 0.0;
                List siteList = ContainerReplicaCatalog.getStorageList(file.getName());
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
                switch (ContainerReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        //stage-in job
                        /**
                         * Picks up the site that is closest
                         */
                        double maxRate = Double.MIN_VALUE;
                        for (Storage storage : getStorageList()) {
                            double rate = storage.getMaxTransferRate();
                            if (rate > maxRate) {
                                maxRate = rate;
                            }
                        }
                        //Storage storage = getStorageList().get(0);
                        time += file.getSize() / (double) Consts.MILLION / maxRate;
                        break;
                    case LOCAL:
                        int vmId = job.getVmId();
                        int userId = job.getUserId();
                        ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
                        ContainerVm vm = host.getContainerVm(vmId, userId);

                        boolean requiredFileStagein = true;
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
                                requiredFileStagein = false;
                                break;
                            }
                            double bwth;
                            if (site.equals(ContainerParameters.SOURCE)) {
                                //transfers from the source to the VM is limited to the VM bw only
                                bwth = vm.getBw();
                                //bwth = dcStorage.getBaseBandwidth();
                            } else {
                                //transfers between two VMs is limited to both VMs
                                bwth = Math.min(vm.getBw(), getVmAllocationPolicy().getHost(Integer.parseInt(site), userId).getContainerVm(Integer.parseInt(site), userId).getBw());
                                //bwth = dcStorage.getBandwidth(Integer.parseInt(site), vmId);
                            }
                            if (bwth > maxBwth) {
                                maxBwth = bwth;
                            }
                        }
                        if (requiredFileStagein && maxBwth > 0.0) {
                            time += file.getSize() / (double) Consts.MILLION / maxBwth;
                        }

                        /**
                         * For the case when storage is too small it is not
                         * handled here
                         */
                        //We should add but since CondorVm has a small capability it often fails
                        //We currently don't use this storage to do anything meaningful. It is left for future.
                        //condorVm.addLocalFile(file);
                        ContainerReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
        return time;
    }

    @Override
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        //this is a bug of CloudSim if the runtime is smaller than 0.1 (now is 0.01) it doesn't work at all
        if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.01) {
            List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
            double smallerTime = Double.MAX_VALUE;
            // for each host...
            for (ContainerHost host : list) {
                // inform VMs to update processing
                double time = host.updateContainerVmsProcessing(CloudSim.clock());
                // what time do we expect that the next cloudlet will finish?
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }
            // gurantees a minimal interval before scheduling the event
            if (smallerTime < CloudSim.clock() + 0.11) {
                smallerTime = CloudSim.clock() + 0.11;
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());
        }
    }

    @Override
    protected void checkCloudletCompletion() {
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm vm : host.getVmList()) {
                for (Container container : vm.getContainerList()) {
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet cl = container.getContainerCloudletScheduler().getNextFinishedCloudlet();
                        if (cl != null) {
                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                            register(cl);
                        }
                    }
                }
            }
        }
    }

    /*
     * Register a file to the storage if it is an output file
     * @param requiredFiles, all files to be stage-in
     * @param job, the job to be processed
     * @pre  $none
     * @post $none
     */
    private void register(Cloudlet cl) {
        ContainerTask tl = (ContainerTask) cl;
        List<ContainerFileItem> fList = tl.getFileList();
        for (ContainerFileItem file : fList) {
            if (file.getType() == ContainerParameters.FileType.OUTPUT)//output file
            {
                switch (ContainerReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        ContainerReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                        break;
                    case LOCAL:
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
                        /**
                         * Left here for future work
                         */
                        ContainerCondorVM vm = (ContainerCondorVM) host.getContainerVm(vmId, userId);
                        ContainerReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
    }
}

