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

import java.io.File;
import java.io.FileOutputStream;
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
//            Log.disable();
//            FileOutputStream fos = null;
//            File file;
//            file = new File("E:\\term12\\Dataset\\log.txt");
//            fos = new FileOutputStream(file);
//            if (!file.exists()) {
//                file.createNewFile();
//            }
//            Log.setOutput(fos);
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
//            ContainerAllocationPolicy containerAllocationPolicy = new ContainerAllocationPolicySimple();
            ContainerAllocationPolicy containerAllocationPolicy = new PowerContainerAllocationPolicySimple();


            double overUtilizationThreshold = 0.80;
            double underUtilizationThreshold = 0.70;
            hostList = new ArrayList<ContainerHost>();
            hostList = createHostList(Parameters.HOST_NUMBERS);

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
            printStatus(workflowEngine, broker);

            CloudSim.stopSimulation();
            // print informations
            Log.printLine("\n");
            Log.printLine("Wass Example finished!");
        } catch (Exception e) {
        Log.printLine("The simulation has been terminated due to an unexpected error");
        e.printStackTrace();
        }
    }

    private static void printStatus(WorkflowEngine workflowEngine, WorkflowDatacenterBroker broker){
        int PSR = 0;
        int DeadlineSuccess = 0;
        double accumulatedWorkflowCost = 0;
        double accumulatedWorkflowBudget = 0;
        double accumulatedCostToBudget = 0;
        double accumulatedMakeSpanToDeadline = 0;
        int totalVmNumbers = broker.getVmsDestroyedList().size() + broker.getVmsCreatedList().size();
        double totalVmCost = 0;
        int totalWorkflowNumber = workflowEngine.getWorkflowList().size();

        for (Workflow workflow: workflowEngine.getWorkflowList()) {
            Log.printLine("--------------"+ workflow.getName()+ "-------------------");
            double makeSpan =  workflow.getCurrentMakeSpan();
            Log.printConcatLine("Deadline:", workflow.getDeadline(), "\nMake Span:", makeSpan + "\n");
            Log.printConcatLine("Budget:", workflow.getBudget(), "\nCost:", workflow.getTotalCost() + "\n");
            accumulatedWorkflowCost +=  workflow.getTotalCost();
            accumulatedWorkflowBudget += workflow.getBudget();
            accumulatedCostToBudget += workflow.getTotalCost() / workflow.getBudget();
            accumulatedMakeSpanToDeadline += makeSpan / workflow.getDeadline();

            if (workflow.getCurrentMakeSpan() <= workflow.getDeadline() && workflow.getTotalCost() <= workflow.getBudget()){
                PSR++;
            }
            if (workflow.getCurrentMakeSpan() <= workflow.getDeadline()){
                DeadlineSuccess++;
            }

        }

        Log.printConcatLine("\nPSR: ", (double)PSR *100 / totalWorkflowNumber, "%");
        Log.printConcatLine("Deadline success:  ", (double)DeadlineSuccess  * 100 / totalWorkflowNumber , "%");
        Log.printConcatLine("Accumulated Workflow Cost: ", accumulatedWorkflowCost);
        Log.printConcatLine("Average Workflow Cost: ", accumulatedWorkflowCost / totalWorkflowNumber);
        Log.printConcatLine("Accumulated Workflow Budget: ", accumulatedWorkflowBudget);
        Log.printConcatLine("Average Workflow Budget: ", accumulatedWorkflowBudget / totalWorkflowNumber);
        Log.printConcatLine("Average Cost to Budget:    ", (accumulatedCostToBudget / totalWorkflowNumber));
        Log.printConcatLine("Average MakeSpan to Deadline:  ", (accumulatedMakeSpanToDeadline / totalWorkflowNumber));
        Log.printConcatLine("TOTAL VM NUMBERS:  ", totalVmNumbers);
        Log.printConcatLine("TOTAL  Destroyed VM NUMBERS: ", broker.getVmsDestroyedList().size());
        Log.printConcatLine("TOTAL Created VM NUMBERS: ", broker.getVmsCreatedList().size());

//        for(ContainerVm vm : broker.getVmsDestroyedList()){
//            CondorVM castedVm = (CondorVM) vm;
//            double time = castedVm.getReleaseTime() - castedVm.getLeaseTime();
//            totalVmCost +=  castedVm.getCost() * Math.ceil( time / Parameters.BILLING_PERIOD);
//        }
//        for(ContainerVm vm : broker.getVmsCreatedList()){
//            CondorVM castedVm = (CondorVM) vm;
//            double time = castedVm.getReleaseTime() - castedVm.getLeaseTime();
//            totalVmCost +=  castedVm.getCost() * Math.ceil( time / Parameters.BILLING_PERIOD);
//        }
//        Log.printConcatLine("TOTAL VM Cost: ", totalVmCost);
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
