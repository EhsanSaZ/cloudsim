package org.containerWorkflowsimDemo.scheduling;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.containerWorkflowsimDemo.ContainerCondorVM;

import java.util.HashMap;
import java.util.Map;

//TODO EHSAN : ADD SUPPORT FOR CONTAINER
public class ContainerStaticSchedulingAlgorithm extends ContainerBaseSchedulingAlgorithm{
    public ContainerStaticSchedulingAlgorithm() {
        super();
    }
    @Override
    //TODO EHSAN IMPLEMENT THIS METHOD
    public void run() throws Exception {

        Map<Integer, ContainerCondorVM> mId2Vm = new HashMap<>();
        Map<Integer, Container> mId2container = new HashMap<>();

        for (int i = 0; i < getVmList().size(); i++) {
            ContainerCondorVM vm = (ContainerCondorVM) getVmList().get(i);
            if (vm != null) {
                mId2Vm.put(vm.getId(), vm);
            }
        }

        for (int i = 0; i < getContainerList().size(); i++) {
            Container container = (Container) getContainerList().get(i);
            if (container != null) {
                mId2container.put(container.getId(), container);
            }
        }

        int size = getCloudletList().size();

        for (int i = 0; i < size; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            /**
             * Make sure cloudlet is matched to a VM. It should be done in the
             * Workflow Planner. If not, throws an exception because
             * StaticSchedulingAlgorithm itself does not do the mapping.
             */
//            if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {
//                Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
//                        + "It is possible a stage-in job");
//                cloudlet.setVmId(1);
//                cloudlet.setContainerId(1);
//            }

            if (cloudlet.getContainerId() < 0 || !mId2container.containsKey(cloudlet.getContainerId())) {
                Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                        + "It is possible a stage-in job");
                cloudlet.setVmId(1);
                cloudlet.setContainerId(1);
            }

            Container container = mId2container.get(cloudlet.getContainerId());
            ContainerCondorVM vm = (ContainerCondorVM) container.getVm();
//            ContainerCondorVM vm = mId2Vm.get(cloudlet.getVmId());
            cloudlet.setVmId(vm.getId());
//            if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
//                vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                getScheduledList().add(cloudlet);
                Log.printLine("Schedules " + cloudlet.getCloudletId() + " with "
                        + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId() + "on Container" + cloudlet.getContainerId()) ;
//            }
        }


//        getScheduledList().add(getCloudletList());
    }
}
