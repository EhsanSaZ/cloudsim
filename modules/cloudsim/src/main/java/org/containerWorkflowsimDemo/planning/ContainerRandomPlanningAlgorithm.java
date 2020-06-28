package org.containerWorkflowsimDemo.planning;

import org.cloudbus.cloudsim.container.core.Container;
import org.containerWorkflowsimDemo.ContainerCondorVM;
import org.containerWorkflowsimDemo.ContainerTask;
import org.containerWorkflowsimDemo.utils.ContainerParameters;

import java.util.Iterator;
import java.util.Random;

public class ContainerRandomPlanningAlgorithm extends ContainerBasePlanningAlgorithm {
    @Override
    public void run() {

        Random random = new Random(System.currentTimeMillis());
        for (Iterator it = getTaskList().iterator(); it.hasNext(); ) {
            ContainerTask task = (ContainerTask) it.next();
            double duration = task.getCloudletLength() / 1000;

            for (int i = 0; i < task.getParentList().size(); i++) {
                ContainerTask parent = task.getParentList().get(i);
            }


            for (int i = 0; i < task.getChildList().size(); i++) {
                ContainerTask child = task.getChildList().get(i);
            }

//            int vmNum = getVmList().size();
            int containerNum = getContainerList().size();
            /**
             * Randomly choose a vm
             */

//            int vmId = random.nextInt(vmNum);
            int containerId = random.nextInt(containerNum);

//            CondorVM vm = (CondorVM) getVmList().get(vmId);
            Container c = (Container) getContainerList().get(containerId);
            ContainerCondorVM vm = (ContainerCondorVM) c.getVm();

            //This shows the cpu capability of a vm
            double mips = c.getMips();

//            task.setVmId(vm.getId());
            task.setContainerId(c.getId());

            long deadline = ContainerParameters.getDeadline();

        }
    }
}
