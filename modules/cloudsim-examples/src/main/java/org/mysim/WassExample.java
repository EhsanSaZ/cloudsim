package org.mysim;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.*;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
//import org.containerWorkflowsimDemo.ConstantsExamples;
//import org.containerWorkflowsimDemo.ContainerWorkflowDatacenter;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.examples.container.ConstantsExamples;
import org.mysim.budgetdistribution.BudgetDistributionStrategySpareBudget;
import org.mysim.deadlinedistribution.DeadlineDistributionSimpleUpwardRank;
import org.mysim.planning.MyPlanningAlgorithm;
import org.mysim.planning.PlanningAlgorithmStrategy;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public class WassExample {

    private static List<ContainerCloudlet> cloudletList;

    private static List<CondorVM> vmList;

    private static List<Container> containerList;

    private static List<ContainerHost> hostList;

    public static void main(String[] args) {
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            ReplicaCatalog.init(file_system);

            Parameters.init("E:\\term12\\Dataset\\test workload");

            // create all allocations and distributions strategy
            DeadlineDistributionSimpleUpwardRank ddDistribution = new DeadlineDistributionSimpleUpwardRank();

            BudgetDistributionStrategySpareBudget bDistribution = new BudgetDistributionStrategySpareBudget();

//            ContainerAllocationPolicy containerAllocationPolicy = new ContainerAllocationPolicyRS();
            ContainerAllocationPolicy containerAllocationPolicy = new ContainerAllocationPolicySimple();

            double overUtilizationThreshold = 0.80;
            double underUtilizationThreshold = 0.70;
            hostList = new ArrayList<ContainerHost>();
            hostList = createHostList(20);

            ContainerVmAllocationPolicy vmAllocationPolicy = new ContainerVmAllocationPolicySimple(hostList);


            WorkflowEngine workflowEngine = new WorkflowEngine("workflow_engine");

            int overBookingFactor = 80;
            WorkflowDatacenterBroker broker = createBroker(overBookingFactor, workflowEngine.getId());
            int brokerId = broker.getId();

            WorkflowParser workflowParser = new WorkflowParser(brokerId);

            workflowEngine.setWorkflowParser(workflowParser);
            workflowEngine.setBroker(broker);
            workflowEngine.setDeadlineDistributor(ddDistribution);
            workflowEngine.setBudgetDistributor(bDistribution);

            PlanningAlgorithmStrategy myPlanner= new MyPlanningAlgorithm();

            workflowEngine.setPlanner(myPlanner);

            String logAddress = "~/Results";

            WorkflowContainerDatacenter datacenter = createDatacenter(
                    "datacenter",WorkflowContainerDatacenter.class,hostList,
                    vmAllocationPolicy,
                    containerAllocationPolicy,
                    getExperimentName("WassExample", String.valueOf(overBookingFactor)),
                    Parameters.CONTAINER_VM_SCHEDULING_INTERVAL, logAddress,
                    Parameters.VM_PROVISIONING_DELAY, Parameters.CONTAINER_PROVISIONING_DELAY);

            broker.bindSchedulerDatacenter(datacenter.getId());

            CloudSim.startSimulation();

            // get all data that  we need from engine and broker...
            System.out.println(workflowEngine.getWorkflowList().size());

            CloudSim.stopSimulation();
            // print informations
            Log.printLine("ContainerCloudSimExample1 finished!");
        } catch (Exception e) {
        Log.printLine("The simulation has been terminated due to an unexpected error");
        e.printStackTrace();
        }
    }

    private static WorkflowDatacenterBroker createBroker(int overBookingFactor, int workflowEngineId) {

        WorkflowDatacenterBroker broker = null;

        try {
            broker = new WorkflowDatacenterBroker("Broker", overBookingFactor, workflowEngineId);
        } catch (Exception var2) {
            var2.printStackTrace();
            System.exit(0);
        }

        return broker;
    }

    private static String getExperimentName(String... args) {
        StringBuilder experimentName = new StringBuilder();

        for (int i = 0; i < args.length; ++i) {
            if (!args[i].isEmpty()) {
                if (i != 0) {
                    experimentName.append("_");
                }

                experimentName.append(args[i]);
            }
        }

        return experimentName.toString();
    }

    public static List<ContainerHost> createHostList(int hostsNumber) {
        ArrayList<ContainerHost> hostList = new ArrayList<ContainerHost>();
        for (int i = 0; i < hostsNumber; ++i) {
//            int hostType = i / (int) Math.ceil((double) hostsNumber / 3.0D);
            int hostType = 0;
            ArrayList<ContainerVmPe> peList = new ArrayList<ContainerVmPe>();
            for (int j = 0; j < Parameters.HOST_PES[hostType]; ++j) {
                peList.add(new ContainerVmPe(j, new ContainerVmPeProvisionerSimple((double) Parameters.HOST_MIPS[hostType])));
            }
//            hostList.add(new PowerContainerHostUtilizationHistory(IDs.pollId(ContainerHost.class),
//                    new ContainerVmRamProvisionerSimple(ConstantsExamples.HOST_RAM[hostType]),
//                    new ContainerVmBwProvisionerSimple(1000000L),
//                    1000000L, peList,
//                    new ContainerVmSchedulerTimeSharedOverSubscription(peList),
//                    ConstantsExamples.HOST_POWER[hostType]));

            hostList.add(new ContainerHost(IDs.pollId(ContainerHost.class),
                    new ContainerVmRamProvisionerSimple(Parameters.HOST_RAM[hostType]),
                    new ContainerVmBwProvisionerSimple(1000000L),
                    Parameters.HOST_STORAGE, peList,
                    new ContainerVmSchedulerTimeSharedOverSubscription(peList)));
        }
        return hostList;
    }

    public static WorkflowContainerDatacenter createDatacenter(String name,
                                                               Class<? extends ContainerDatacenter> datacenterClass,
                                                               List<ContainerHost> hostList,
                                                               ContainerVmAllocationPolicy vmAllocationPolicy,
                                                               ContainerAllocationPolicy containerAllocationPolicy,
                                                               String experimentName, double schedulingInterval,
                                                               String logAddress, double VMStartupDelay,
                                                               double ContainerStartupDelay) throws Exception {
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0D;
        double cost = 3.0D;
        double costPerMem = 0.05D;
        double costPerStorage = 0.001D;
        double costPerBw = 0.0D;
        ContainerDatacenterCharacteristics characteristics = new
                ContainerDatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage,
                costPerBw);
        WorkflowContainerDatacenter datacenter = new WorkflowContainerDatacenter(name, characteristics, vmAllocationPolicy,
                containerAllocationPolicy, new LinkedList<Storage>(), schedulingInterval, experimentName, logAddress);

        return datacenter;
    }


}
