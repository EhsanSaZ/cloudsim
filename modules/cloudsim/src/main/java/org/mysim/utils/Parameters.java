package org.mysim.utils;

import java.util.List;

public class Parameters {

    // TODO EHSAN: SCALE OF MEMORY AND RUNTIME NEEDED TO BE CHECKED
    private static double runtime_scale = 1.0;
    private static double peakMemory_Scale = 1.0;

    private static String workflowsDirectory;

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
    public static String SOURCE = "source";

    public static String getWorkflowsDirectory() {
        return workflowsDirectory;
    }


    public static double getRuntimeScale() {
        return runtime_scale;
    }

    public static double getPeakMemoryScale() {
        return peakMemory_Scale;
    }

    public static void init(String workflowsPath) {
        workflowsDirectory = workflowsPath;

    }


    public static final int VM_TYPES_NUMBERS = 4;
    public static final double[] VM_MIPS = new double[]{1000, 1000, 1000, 1000};
    public static final int[] VM_PES = new int[]{1, 2, 4, 8};
    public static final float[] VM_RAM = new float[]{(float) 1024, (float) 2048, (float) 4096, (float) 8192};//**MB*
    public static final int VM_BW = 100000;// BYTE OR BIT OR MB...
    public static final double COST[] = new double[]{3, 6, 9, 12};
    public static final double COST_PER_MEM[] = new double[]{1, 2, 3, 4};
    public static final double COST_PER_STORAGE[] = new double[]{1, 2, 3, 4};
    public static final double COST_PER_BW[] = new double[]{1, 2, 3, 4};

    public static final int VM_SIZE = 2500;

    public static final int R_T_Q_SCHEDULING_INTERVAL = 100;
    public static final int CHECK_FINISHED_STATUS_DELAY = 200;

}
