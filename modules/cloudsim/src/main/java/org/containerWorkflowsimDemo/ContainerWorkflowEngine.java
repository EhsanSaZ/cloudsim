package org.containerWorkflowsimDemo;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.containerWorkflowsimDemo.reclustering.ContainerReclusteringEngine;
import org.containerWorkflowsimDemo.utils.ContainerParameters;


import org.workflowsim.WorkflowSimTags;



import java.util.*;

public class ContainerWorkflowEngine extends SimEntity {
    /**
     * The job list.
     */
    protected List<? extends ContainerCloudlet> jobsList;
    /**
     * The job submitted list.
     */
    protected List<? extends ContainerCloudlet> jobsSubmittedList;
    /**
     * The job received list.
     */
    protected List<? extends ContainerCloudlet> jobsReceivedList;
    /**
     * The job submitted.
     */
    protected int jobsSubmitted;
    protected List<? extends ContainerVm> vmList;
    protected List<? extends Container> containerList;
    /**
     * The associated scheduler id*
     */
    private List<Integer> schedulerId;
    private List<ContainerWorkflowScheduler> scheduler;

    public ContainerWorkflowEngine(String name, double overBookingfactor) throws Exception {
        this(name,1,overBookingfactor);
    }

    public ContainerWorkflowEngine(String name, int schedulers, double overBookingfactor) throws Exception {
        super(name);
        setJobsList(new ArrayList<>());
        setJobsSubmittedList(new ArrayList<>());
        setJobsReceivedList(new ArrayList<>());

        jobsSubmitted = 0;

        setSchedulers(new ArrayList<>());
        setSchedulerIds(new ArrayList<>());

        for (int i = 0; i < schedulers; i++) {
            ContainerWorkflowScheduler wfs = new ContainerWorkflowScheduler(name + "_Scheduler_" + i, overBookingfactor);
            getSchedulers().add(wfs);
            getSchedulerIds().add(wfs.getId());
            wfs.setWorkflowEngineId(this.getId());
        }
    }


    public void submitVmList(List<? extends ContainerVm> list, int schedulerId) {
        getScheduler(schedulerId).submitVmList(list);
    }

    public void submitVmList(List<? extends ContainerVm> list) {
        //bug here, not sure whether we should have different workflow schedulers
        getScheduler(0).submitVmList(list);
        setVmList(list);
    }

    public void submitContainerList(List<? extends Container> list, int schedulerId){
        getScheduler(schedulerId).submitContainerList(list);
    }

    public void submitContainerList(List<? extends Container> list){
        //bug here, not sure whether we should have different workflow schedulers
        getScheduler(0).submitContainerList(list);
        setContainerList(list);
    }

    public List<? extends ContainerVm> getAllVmList() {
        if (this.vmList != null && !this.vmList.isEmpty()) {
            return this.vmList;
        } else {
            List list = new ArrayList();
            for (int i = 0; i < getSchedulers().size(); i++) {
                list.addAll(getScheduler(i).getVmList());
            }
            return list;
        }
    }

    public List<? extends Container> getAllContainerList() {
        if (this.containerList != null && !this.containerList.isEmpty()) {
            return this.containerList;
        } else {
            List list = new ArrayList();
            for (int i = 0; i < getSchedulers().size(); i++) {
                list.addAll(getScheduler(i).getContainerList());
            }
            return list;
        }
    }

    /**
     * This method is used to send to the broker the list of cloudlets.
     */
    public void submitCloudletList(List<? extends ContainerCloudlet> list) {
        getJobsList().addAll(list);
    }

    @Override
    public void processEvent(SimEvent ev) {
        // TODO EHSAN add needed extra container tags and processors....
        // TODO EHSAN change workflowsim tags to containerworkflowsim tags
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            //this call is from workflow scheduler when all vms are created
            case CloudSimTags.CLOUDLET_SUBMIT:
                submitJobs();
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processJobReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                processJobSubmit(ev);
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        for (int i = 0; i < getSchedulerIds().size(); i++) {
            schedule(getSchedulerId(i), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
        }
    }

    protected void submitJobs() {
        List<ContainerJob> list = getJobsList();
        Map<Integer, List> allocationList = new HashMap<>();
        for (int i = 0; i < getSchedulers().size(); i++) {
            List<ContainerJob> submittedList = new ArrayList<>();
            allocationList.put(getSchedulerId(i), submittedList);
        }
        int num = list.size();
        for (int i = 0; i < num; i++) {
            ContainerJob job = list.get(i);
            //Dont use job.isFinished() it is not right
            if (!hasJobListContainsID(this.getJobsReceivedList(), job.getCloudletId())) {
                List<ContainerJob> parentList = job.getParentList();
                boolean flag = true;
                for (ContainerJob parent : parentList) {
                    if (!hasJobListContainsID(this.getJobsReceivedList(), parent.getCloudletId())) {
                        flag = false;
                        break;
                    }
                }
                /**
                 * This job's parents have all completed successfully. Should
                 * submit.
                 */
                if (flag) {
                    List submittedList = allocationList.get(job.getUserId());
                    submittedList.add(job);
                    jobsSubmitted++;
                    getJobsSubmittedList().add(job);
                    list.remove(job);
                    i--;
                    num--;
                }
            }
        }
        /**
         * If we have multiple schedulers. Divide them equally.
         */
        for (int i = 0; i < getSchedulers().size(); i++) {
            List submittedList = allocationList.get(getSchedulerId(i));
            //divid it into sublist

            int interval = ContainerParameters.getOverheadParams().getWEDInterval();
            double delay = 0.0;
            if(ContainerParameters.getOverheadParams().getWEDDelay()!=null){
                delay = ContainerParameters.getOverheadParams().getWEDDelay(submittedList);
            }
            double delaybase = delay;
            int size = submittedList.size();
            if (interval > 0 && interval <= size) {
                int index = 0;
                List subList = new ArrayList();
                while (index < size) {
                    subList.add(submittedList.get(index));
                    index++;
                    if (index % interval == 0) {
                        //create a new one
                        schedule(getSchedulerId(i), delay, CloudSimTags.CLOUDLET_SUBMIT, subList);
                        delay += delaybase;
                        subList = new ArrayList();
                    }
                }
                if (!subList.isEmpty()) {
                    schedule(getSchedulerId(i), delay, CloudSimTags.CLOUDLET_SUBMIT, subList);
                }
            } else if (!submittedList.isEmpty()) {
                sendNow(this.getSchedulerId(i), CloudSimTags.CLOUDLET_SUBMIT, submittedList);
            }
        }
    }

    protected void processJobReturn(SimEvent ev) {
        ContainerJob job = (ContainerJob) ev.getData();
        if (job.getCloudletStatus() == Cloudlet.FAILED) {
            // Reclusteringengine will add retry job to jobList
            int newId = getJobsList().size() + getJobsSubmittedList().size();
            //TODO EHSAN implement reclustring engine...
            getJobsList().addAll(ContainerReclusteringEngine.process(job, newId));
        }

        getJobsReceivedList().add(job);
        jobsSubmitted--;
        if (getJobsList().isEmpty() && jobsSubmitted == 0) {
            //send msg to all the schedulers
            for (int i = 0; i < getSchedulerIds().size(); i++) {
                sendNow(getSchedulerId(i), CloudSimTags.END_OF_SIMULATION, null);
            }
        } else {
            sendNow(this.getId(), CloudSimTags.CLOUDLET_SUBMIT, null);
        }
    }

    protected void processJobSubmit(SimEvent ev) {
        List<? extends ContainerCloudlet> list = (List) ev.getData();
        setJobsList(list);
    }

    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
            return;
        }
        Log.printLine(getName() + ".processOtherEvent(): "
                + "Error - event unknown by this DatacenterBroker.");
    }

    public void bindSchedulerDatacenter(int datacenterId, int schedulerId) {
        getScheduler(schedulerId).bindSchedulerDatacenter(datacenterId);
    }

    public void bindSchedulerDatacenter(int datacenterId) {
        bindSchedulerDatacenter(datacenterId, 0);
    }

    private boolean hasJobListContainsID(List jobList, int id) {
        for (Iterator it = jobList.iterator(); it.hasNext();) {
            ContainerJob job = (ContainerJob) it.next();
            if (job.getCloudletId() == id) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }

    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
    }

    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsList() {
        return (List<T>) jobsList;
    }

    private <T extends ContainerCloudlet> void setJobsList(List<T> jobsList) {
        this.jobsList = jobsList;
    }

    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsSubmittedList() {
        return (List<T>) jobsSubmittedList;
    }

    private <T extends ContainerCloudlet> void setJobsSubmittedList(List<T> jobsSubmittedList) {
        this.jobsSubmittedList = jobsSubmittedList;
    }

    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsReceivedList() {
        return (List<T>) jobsReceivedList;
    }

    private <T extends ContainerCloudlet> void setJobsReceivedList(List<T> jobsReceivedList) {
        this.jobsReceivedList = jobsReceivedList;
    }

    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getVmList() {
        return (List<T>) vmList;
    }

    private <T extends ContainerVm> void setVmList(List<T> vmList) {
        this.vmList = vmList;
    }

    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }

    private <T extends Container> void setContainerList(List<T> containerList) {
        this.containerList = containerList;
    }

    public List<ContainerWorkflowScheduler> getSchedulers() {
        return this.scheduler;
    }

    private void setSchedulers(List list) {
        this.scheduler = list;
    }


    public List<Integer> getSchedulerIds() {
        return this.schedulerId;
    }

    private void setSchedulerIds(List list) {
        this.schedulerId = list;
    }

    public int getSchedulerId(int index) {
        if (this.schedulerId != null) {
            return this.schedulerId.get(index);
        }
        return 0;
    }
    public ContainerWorkflowScheduler getScheduler(int schedulerId) {
        if (this.scheduler != null) {
            return this.scheduler.get(schedulerId);
        }
        return null;
    }
}
