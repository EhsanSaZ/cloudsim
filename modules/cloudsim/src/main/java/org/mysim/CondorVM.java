package org.mysim;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.utils.MySimTags;
import org.mysim.utils.VmStateEntry;
//import org.workflowsim.WorkflowSimTags;

import java.util.LinkedList;
import java.util.List;

public class CondorVM extends PowerContainerVm {
//    private int state;
//    private boolean isScheduled;
    private final List<VmStateEntry> busyStateHistory = new LinkedList<VmStateEntry>();
    /**
     * The previous time.
     */
    private double previousBusyStateCheckTime;

    private double costPerMem = 0.0;
    private double costPerBW = 0.0;
    private double costPerStorage = 0.0;
    private double cost = 0.0;

    private double leaseTime = -1;
    private double releaseTime = -1;

    private int availablePeNumbersForSchedule;
    private double availableRamForSchedule;
    private long availableSizeForSchedule;

    private  double memPerCore;

    private int lastPaidPeriod;
    private int gratisPesNumber;
    private  int usedGratisPesNumber;

    private long intervalIndex;
    private double averageCpuUtilization;

    public CondorVM(final int id, final int userId, final double mips, final float ram, final long bw, final long size, final String vmm,
                    final ContainerScheduler containerScheduler, final ContainerRamProvisioner containerRamProvisioner, final ContainerBwProvisioner containerBwProvisioner,
                    List<? extends ContainerPe> peList, final double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner,
                peList, schedulingInterval);
//        setState(MySimTags.VM_STATUS_IDLE);
        setAvailablePeNumbersForSchedule(peList.size());
        setAvailableRamForSchedule(ram);
        setAvailableSizeForSchedule(size);
        setMemPerCore(ram/peList.size());
        setLastPaidPeriod(-1);
        setGratisPesNumber(0);
        setUsedGratisPesNumber(0);
        setAverageCpuUtilization(0);
        setIntervalIndex(0);
    }

    public CondorVM(final int id, final int userId, final double mips, final float ram, final long bw, final long size, final String vmm,
                    final ContainerScheduler containerScheduler, final ContainerRamProvisioner containerRamProvisioner, final ContainerBwProvisioner containerBwProvisioner,
                    List<? extends ContainerPe> peList, final double schedulingInterval,
                    double cost, double costPerMem, double costPerStorage, double costPerBW) {
        this(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner,
            peList, schedulingInterval);
        setCost(cost);
        setCostPerMem(costPerMem);
        setCostPerStorage(costPerStorage);
        setCostPerBW(costPerBW);
//        setState(MySimTags.VM_STATUS_IDLE);
        setAvailablePeNumbersForSchedule(peList.size());
        setAvailableRamForSchedule(ram);
        setAvailableSizeForSchedule(size);
        setMemPerCore(ram/peList.size());
        setLastPaidPeriod(-1);
        setGratisPesNumber(0);
        setUsedGratisPesNumber(0);
        setAverageCpuUtilization(0);
        setIntervalIndex(0);
    }

    @Override
    public double updateVmProcessing(final double currentTime, final List<Double> mipsShare) {
        double time = super.updateVmProcessing(currentTime, mipsShare);
        //currentTime > getPreviousBusyStateCheckTime() && (currentTime - 0.2) % getSchedulingInterval() == 0
        if ((currentTime - getPreviousBusyStateCheckTime()) >= getSchedulingInterval()) {
            if (CloudSim.clock() != 0 && getContainerList().size() > 0){
                addBusyStateHistory(currentTime, MySimTags.VM_STATUS_BUSY);
//                setState(MySimTags.VM_STATUS_BUSY);

            }else if(CloudSim.clock() != 0 && getContainerList().size() == 0){
                addBusyStateHistory(currentTime, MySimTags.VM_STATUS_IDLE);
//                setState(MySimTags.VM_STATUS_IDLE);
            }
            setPreviousBusyStateCheckTime(currentTime);

            // calculate average utilization
//            double busyPEsNumber = 0;
//            for (Container container : getContainerList()) {
//                if(!getContainersMigratingIn().contains(container)) {
//                    time = container.getContainerCloudletScheduler().getPreviousTime();
//                    //container.getTotalUtilizationOfCpu(time) is always 1 for each cloudlet running on this container
//                    // because it is set to full utilization model on task creation.
//                    busyPEsNumber += container.getTotalUtilizationOfCpu(time) * container.getNumberOfPes();
//                }
//            }
            double currentUtilization = ((double)getNumberOfPes() - getAvailablePeNumbersForSchedule()) / getNumberOfPes() ;

            setAverageCpuUtilization( ( ( getIntervalIndex() * getAverageCpuUtilization() ) + currentUtilization ) / (getIntervalIndex() + 1));
            setIntervalIndex( getIntervalIndex() + 1);

        }

        return time;
    }

    public void addBusyStateHistory( double time, int state){
        VmStateEntry newState = new VmStateEntry(time, state);
        getBusyStateHistory().add(0, newState);
        if (getBusyStateHistory().size() > HISTORY_LENGTH) {
            getBusyStateHistory().remove(HISTORY_LENGTH);
        }

//        if (!getBusyStateHistory().isEmpty()){
//            VmStateEntry previousState = getBusyStateHistory().get(getBusyStateHistory().size() - 1);
//            if (previousState.getTime() == time) {
//                getBusyStateHistory().set(getBusyStateHistory().size() - 1, newState);
//                return;
//            }
//        }
//        getBusyStateHistory().add(newState);
    }
    public boolean isSuitableForTask(int requiredPeNumbers, int requiredMemory){
        //T ODO EHSAN: Implement this
        return getAvailablePeNumbersForSchedule() >= requiredPeNumbers &&
                getAvailableRamForSchedule() >= requiredMemory;
//        ContainerSchedulerTimeShared scheduler = (ContainerSchedulerTimeShared) getContainerScheduler();
//        scheduler.getPesInUse();
//        if ( task.getNumberOfPes() > (getPeList().size() - scheduler.getPesInUse()) || )
    }
    //----------------setter and getter

//    public int getState() {
//        return state;
//    }

//    public void setState(int state) {
//        this.state = state;
//    }

    public List<VmStateEntry> getBusyStateHistory() {
        return busyStateHistory;
    }

    public double getPreviousBusyStateCheckTime() {
        return previousBusyStateCheckTime;
    }

    public void setPreviousBusyStateCheckTime(double previousBusyStateCheckTime) {
        this.previousBusyStateCheckTime = previousBusyStateCheckTime;
    }

    public double getCostPerMem() {
        return costPerMem;
    }

    public void setCostPerMem(double costPerMem) {
        this.costPerMem = costPerMem;
    }

    public double getCostPerBW() {
        return costPerBW;
    }

    public void setCostPerBW(double costPerBW) {
        this.costPerBW = costPerBW;
    }

    public double getCostPerStorage() {
        return costPerStorage;
    }

    public void setCostPerStorage(double costPerStorage) {
        this.costPerStorage = costPerStorage;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getLeaseTime() {
        return leaseTime;
    }

    public double getReleaseTime() {
        return releaseTime;
    }

    public void setLeaseTime(double leaseTime) {
        this.leaseTime = leaseTime;
    }

    public void setReleaseTime(double releaseTime) {
        this.releaseTime = releaseTime;
    }

    public int getAvailablePeNumbersForSchedule() {
        return availablePeNumbersForSchedule;
    }

    public void setAvailablePeNumbersForSchedule(int availablePeNumbersForSchedule) {
        this.availablePeNumbersForSchedule = availablePeNumbersForSchedule;
    }

    public double getAvailableRamForSchedule() {
        return availableRamForSchedule;
    }

    public void setAvailableRamForSchedule(double availableRamForSchedule) {
        this.availableRamForSchedule = availableRamForSchedule;
    }

    public long getAvailableSizeForSchedule() {
        return availableSizeForSchedule;
    }

    public void setAvailableSizeForSchedule(long availableSizeForSchedule) {
        this.availableSizeForSchedule = availableSizeForSchedule;
    }

    public double getMemPerCore() { return memPerCore; }

    public void setMemPerCore(double memPerCore) { this.memPerCore = memPerCore; }

    public int getLastPaidPeriod() { return lastPaidPeriod; }

    public void setLastPaidPeriod(int lastPaidPeriod) { this.lastPaidPeriod = lastPaidPeriod; }

    public int getGratisPesNumber() { return gratisPesNumber; }

    public void setGratisPesNumber(int gratisPesNumber) { this.gratisPesNumber = gratisPesNumber; }

    public int getUsedGratisPesNumber() { return usedGratisPesNumber; }

    public void setUsedGratisPesNumber(int usedGratisPesNumber) { this.usedGratisPesNumber = usedGratisPesNumber; }

    public long getIntervalIndex() { return intervalIndex; }

    public void setIntervalIndex(long intervalIndex) { this.intervalIndex = intervalIndex; }

    public double getAverageCpuUtilization() { return averageCpuUtilization; }

    public void setAverageCpuUtilization(double averageCpuUtilization) { this.averageCpuUtilization = averageCpuUtilization; }
}
