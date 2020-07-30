package org.mysim.utils;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.cloudbus.cloudsim.Consts;

import java.util.List;

public class Parameters {

    public enum FileType {
        NONE(0), INPUT(1), OUTPUT(2);
        public final int value;

        private FileType(int fType) {
            this.value = fType;
        }
    }

    public enum ClassType{
        STAGE_IN(1), COMPUTE(2), STAGE_OUT(3), CLEAN_UP(4);
        public final int value;
        private ClassType(int cType){
            this.value = cType;
        }
    }
    /**
     * The cost model
     * DATACENTER: specify the cost per data center
     * VM: specify the cost per VM
     */
    public enum CostModel{
        DATACENTER(1), VM(2);
        public final int value;
        private CostModel(int model){
            this.value = model;
        }
    }

    // TODO EHSAN: SCALE OF MEMORY AND RUNTIME NEEDED TO BE CHECKED
    private static double runtime_scale = 1.0;
    private static double peakMemory_Scale = 1.0;
    private static double minPeakMemory = 10.0  * Consts.MILLION ;// in Byte
    /**
     * The default cost model is based on datacenter, similar to CloudSim
     */
    private static CostModel costModel =CostModel.DATACENTER;

    private static String workflowsDirectory;

    public static String SOURCE = "source";

    public static int BI_FACTOR = 1;
    public static int C_FACTOR = 2;

    public static void init(String workflowsPath) {
        workflowsDirectory = workflowsPath;

    }

    public static String getWorkflowsDirectory() {
        return workflowsDirectory;
    }


    public static double getRuntimeScale() {
        return runtime_scale;
    }
    public static void setRuntime_scale(double runtime_scale) { Parameters.runtime_scale = runtime_scale; }

    public static double getPeakMemoryScale() {
        return peakMemory_Scale;
    }
    public static void setPeakMemory_Scale(double peakMemory_Scale) { Parameters.peakMemory_Scale = peakMemory_Scale; }

    public static double getMinPeakMemory() { return minPeakMemory; }

    public static void setMinPeakMemory(double minPeakMemory) { Parameters.minPeakMemory = minPeakMemory; }

    public static CostModel getCostModel(){ return costModel; }
    public static void setCostModel(CostModel model){
        costModel = model;
    }

    //---------------------------------  VM
    public static final int VM_TYPES_NUMBERS = 4;
    public static final double[] VM_MIPS = new double[]{1000, 1000, 1000, 1000};
    public static final int[] VM_PES = new int[]{1, 2, 4, 8};
    public static final float[] VM_RAM = new float[]{(float) 1024, (float) 2048, (float) 4096, (float) 8192};//**MB*
    public static final int VM_BW = 100000;// Mb/s...
    public static final double COST[] = new double[]{3, 6, 9, 12};
    public static final double COST_PER_MEM[] = new double[]{1, 2, 3, 4};
    public static final double COST_PER_STORAGE[] = new double[]{1, 2, 3, 4};
    public static final double COST_PER_BW[] = new double[]{1, 2, 3, 4};

    public static final int VM_SIZE = 2500;//MB

    //---------------------------------  Container
    public static final double[] CONTAINER_MIPS = new double[]{1000, 1000, 1000, 1000}; // exactly same as vm
    public static final int[] CONTAINER_PES = new int[]{1, 1, 1};
    public static final double[] CONTAINER_RAM = new double[]{128, 256, 512};
    public static final double CONTAINER_BW = 1;
    public static final double CONTAINER_VM_SCHEDULING_INTERVAL = 300.0D;

    public static final int CONTAINER_SIZE = 600;// MB

    //---------------------------------  HOST
    public static final int HOST_TYPES = 1;
    public static final int[] HOST_MIPS = new int[]{37274};
    public static final int[] HOST_PES = new int[]{48};
    public static final int[] HOST_RAM = new int[]{262144};
    public static final int HOST_BW = 1000000;
    public static final long HOST_STORAGE = 1000000;


    public static final int R_T_Q_SCHEDULING_INTERVAL = 100;
    public static final int MONITORING_INTERVAL = 500;
    public static final double VM_THRESH_HOLD_FOR_SHUTDOWN = 60;
    public static final int CHECK_FINISHED_STATUS_DELAY = 200;


    public static final double VM_PROVISIONING_DELAY = 100;
    public static final double VM_DESTROY_DELAY = 0.01;
    public static final double CONTAINER_PROVISIONING_DELAY = 10;
    public static final double CONTAINER_DESTROY_DELAY = 0.01;

    public static final NormalDistribution CPU_DEGRADATION = new NormalDistribution(12,10);
    public static final NormalDistribution BW_DEGRADATION = new NormalDistribution(9.5,5);

    public static final int BILLING_PERIOD = 60;
    public static final double CPU_COST_FACTOR = 0.5;
    public static final double ALPHA_DEADLINE_FACTOR = 0.5;
    public static final double BETA_BUDGET_FACTOR = 0.5;
    public static final double ARRIVAL_RATE = 4;// workflow per minute
}
