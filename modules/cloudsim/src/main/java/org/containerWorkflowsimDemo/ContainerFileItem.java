package org.containerWorkflowsimDemo;

import org.containerWorkflowsimDemo.utils.ContainerParameters;

import java.util.List;

public class ContainerFileItem{

    private String name;

    private double size;

    private ContainerParameters.FileType type;

    public ContainerFileItem(String name, double size) {
        this.name = name;
        this.size = size;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public void setType(ContainerParameters.FileType type) {
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public double getSize() {
        return this.size;
    }

    public ContainerParameters.FileType getType() {
        return this.type;
    }

    /**
     * If a input file has an output file it does not need stage-in For
     * workflows, we have a rule that a file is written once and read many
     * times, thus if a file is an output file it means it is generated within
     * this job and then used by another task within the same job (or other jobs
     * maybe) This is useful when we perform horizontal clustering
     * @param list
     * @return
     */
    public boolean isRealInputFile(List<ContainerFileItem> list) {
        if (this.getType() == ContainerParameters.FileType.INPUT)//input file
        {
            for (ContainerFileItem another : list) {
                if (another.getName().equals(this.getName())
                        /**
                         * if another file is output file
                         */
                        && another.getType() == ContainerParameters.FileType.OUTPUT) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
