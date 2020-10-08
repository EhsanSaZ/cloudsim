package org.mysim;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
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
import org.mysim.budgetdistribution.BudgetDistributionStrategySpareBudget;
import org.mysim.deadlinedistribution.DeadlineDistributionSimpleUpwardRank;
import org.mysim.deadlinedistribution.EPSMDeadlineDistributionAlgorithm;
import org.mysim.deadlinedistribution.RankAndDeadlineDistributionMWHBDCSAlgorithm;
import org.mysim.planning.*;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;
import org.mysim.utils.WorkflowParser;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class WassExample {

    private static List<ContainerCloudlet> cloudletList;

    private static List<CondorVM> vmList;

    private static List<Container> containerList;

    private static List<ContainerHost> hostList;

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("WassSim").build();
        parser.addArgument("-p")
                .type(String.class)
//                .required(true)
                .help("Workflow Directory Path.");
        parser.addArgument("-d")
                .type(Double.class)
                .setDefault(0.5)
                .help("Deadline Factor min value 0. Default is 0.5.");
        parser.addArgument("-b")
                .type(Double.class)
                .setDefault(0.5)
                .help("Budget Factor min value 0. Default is 0.5.");
        parser.addArgument("-a")
                .type(Double.class)
                .setDefault(4.0)
                .help("Arrival Rate number of workflows per minute. Default is 4.0");
        parser.addArgument("-s")
                .type(Double.class)
                .setDefault(1.0)
                .help("Scheduling Interval in Secs. Default is 1");
        parser.addArgument("-m")
                .type(Integer.class)
                .setDefault(60)
                .help("Monitoring Interval in Secs. Default is 60");
        parser.addArgument("-t")
                .type(Double.class)
                .setDefault(300.0)
                .help("Monitoring VM_THRESH_HOLD_FOR_SHUTDOWN to delete vm. Default is 300.0");
        parser.addArgument("-e")
                .type(Boolean.class)
                .setDefault(true)
                .help("Enable Cpu and Network bandwidth degradation. Default is true");
        try {
            Namespace res = parser.parseArgs(args);
//            Log.disable();
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            Date simulationStartDate = new Date() ;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            CloudSim.init(num_user, calendar, trace_flag);

            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            ReplicaCatalog.init(file_system);
            String path = "E:\\term12\\Dataset\\test workload";
//            Parameters.init( res.get("p"), res.get("d"), res.get("b"),res.get("a"), res.get("s"), res.get("m"), res.get("t"), res.get("e"));
            Parameters.init(path, res.get("d"), res.get("b"),res.get("a"), res.get("s"), res.get("m"), res.get("t"), res.get("e"));
            FileOutputStream logFileStream = null;
            if (!new File(Parameters.getWorkflowsDirectory().concat("\\logs")).exists()){
                new File(Parameters.getWorkflowsDirectory().concat("\\logs")).mkdirs();
            }
            File file = new File(String.format(Parameters.getWorkflowsDirectory().concat("\\logs\\%s.txt"),dateFormat.format(simulationStartDate)));
            logFileStream = new FileOutputStream(file);
            if (!file.exists()) {
                file.createNewFile();
            }
            Log.setOutput(logFileStream);

            // DIFFERENT ALGORITHMS
            // create all allocations and distributions strategy
//            DeadlineDistributionSimpleUpwardRank ddDistribution = new DeadlineDistributionSimpleUpwardRank();
            RankAndDeadlineDistributionMWHBDCSAlgorithm ddDistribution = new RankAndDeadlineDistributionMWHBDCSAlgorithm();
//            EPSMDeadlineDistributionAlgorithm ddDistribution = new EPSMDeadlineDistributionAlgorithm();

            // DIFFERENT ALGORITHMS
            BudgetDistributionStrategySpareBudget bDistribution = new BudgetDistributionStrategySpareBudget();

//            ContainerAllocationPolicy containerAllocationPolicy = new ContainerAllocationPolicyRS();
//            ContainerAllocationPolicy containerAllocationPolicy = new ContainerAllocationPolicySimple();
            ContainerAllocationPolicy containerAllocationPolicy = new PowerContainerAllocationPolicySimple();


            double overUtilizationThreshold = 0.80;
            double underUtilizationThreshold = 0.70;
            hostList = new ArrayList<ContainerHost>();
            hostList = createHostList(Parameters.HOST_NUMBERS);

            ContainerVmAllocationPolicy vmAllocationPolicy = new ContainerVmAllocationPolicySimple(hostList);

            // DIFFERENT ALGORITHMS
//            Second_WorkflowEngine workflowEngine = new Second_WorkflowEngine("Second_workflow_engine");
            MHHBDCS_WorkflowEngine workflowEngine = new MHHBDCS_WorkflowEngine("MHHBDCS_workflow_engine");
//            WorkflowEngine workflowEngine = new WorkflowEngine("EPSM_workflow_engine");

//            PlanningAlgorithmStrategy myPlanner= new MyPlanningAlgorithm();
//            PlanningAlgorithmStrategy myPlanner = new MySecondPlanningAlgorithm();

//            PlanningAlgorithmStrategy myPlanner = new MyThirdPlanningAlgorithm();
            PlanningAlgorithmStrategy myPlanner = new MWHBDCS_PlanningAlgorithm();
//            PlanningAlgorithmStrategy myPlanner = new EPSMPlanningAlgorithm();


            int overBookingFactor = 80;
            WorkflowDatacenterBroker broker = createBroker(overBookingFactor, workflowEngine.getId());
            int brokerId = broker.getId();

            WorkflowParser workflowParser = new PegasusWorkflowParser(brokerId);
//            WorkflowParser workflowParser = new WorkflowHubWorkflowParser(brokerId);

            workflowEngine.setWorkflowParser(workflowParser);
            workflowEngine.setBroker(broker);
            workflowEngine.setDeadlineDistributor(ddDistribution);
            workflowEngine.setBudgetDistributor(bDistribution);

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
            Date simulationEndDate = new Date() ;
            SimpleDateFormat logDateFormat = new SimpleDateFormat("HH:mm:ss yyyy-MM-dd") ;
            Log.printConcatLine("Simulation Starts at: ", logDateFormat.format(simulationStartDate));
            Log.printConcatLine("Simulation Ended at: ", logDateFormat.format(simulationEndDate));
            CloudSim.stopSimulation();
            Log.printLine("Wass Example finished!");
        }catch (ArgumentParserException e){
            parser.handleError(e);
        }
        catch (Exception e) {
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
        int totalVmNumbers = broker.getVmDestroyedNumber() + broker.getVmsCreatedList().size();
        double totalVmCost = 0;
        double createdVmsAverageCpuUtilization = 0.0;
        double allVmsAverageCpuUtilization = 0.0;

        int totalWorkflowNumber = workflowEngine.getExecutedWorkflowList().size();

        for (Workflow workflow: workflowEngine.getExecutedWorkflowList()) {
            Log.printLine("--------------"+ workflow.getName()+ "-------------------");
            Log.printConcatLine("Deadline:", workflow.getDeadline(), "\nMake Span:", workflow.getFinalMakeSpan() + "\n");
            Log.printConcatLine("Budget:", workflow.getBudget(), "\nCost:", workflow.getTotalCost() + "\n");
            accumulatedWorkflowCost +=  workflow.getTotalCost();
            accumulatedWorkflowBudget += workflow.getBudget();
            accumulatedCostToBudget += workflow.getTotalCost() / workflow.getBudget();
            accumulatedMakeSpanToDeadline += workflow.getFinalMakeSpan() / workflow.getDeadline();

            if (workflow.getFinalMakeSpan() <= workflow.getDeadline() && workflow.getTotalCost() <= workflow.getBudget()){
                PSR++;
            }
            if (workflow.getFinalMakeSpan() <= workflow.getDeadline()){
                DeadlineSuccess++;
            }

        }
        for (ContainerVm vm : broker.getVmsCreatedList()){
            CondorVM castedVm = (CondorVM) vm;
            createdVmsAverageCpuUtilization += castedVm.getAverageCpuUtilization();
        }
        allVmsAverageCpuUtilization = (broker.getDestroyedVmAverageUtilization() * broker.getVmDestroyedNumber() + createdVmsAverageCpuUtilization) / totalVmNumbers;

        Log.enable();
        Log.printConcatLine("------------------------ Simulation Results ------------------------\n");
        Log.printConcatLine("Total workflows: ", workflowEngine.getWorkflowParser().getTotalWorkflowNumbers());
        Log.printConcatLine("PSR: ", (double)PSR *100 / totalWorkflowNumber, "%");
        Log.printConcatLine("Deadline success:  ", (double)DeadlineSuccess  * 100 / totalWorkflowNumber , "%");
        Log.printConcatLine("Accumulated Workflow Cost: ", accumulatedWorkflowCost);
        Log.printConcatLine("Average Workflow Cost: ", accumulatedWorkflowCost / totalWorkflowNumber);
        Log.printConcatLine("Accumulated Workflow Budget: ", accumulatedWorkflowBudget);
        Log.printConcatLine("Average Workflow Budget: ", accumulatedWorkflowBudget / totalWorkflowNumber);
        Log.printConcatLine("Average Cost to Budget:    ", (accumulatedCostToBudget / totalWorkflowNumber));
        Log.printConcatLine("Average MakeSpan to Deadline:  ", (accumulatedMakeSpanToDeadline / totalWorkflowNumber));
        Log.printConcatLine("TOTAL VM NUMBERS:  ", totalVmNumbers);
        Log.printConcatLine("TOTAL Destroyed VM NUMBERS: ", broker.getVmDestroyedNumber());
        Log.printConcatLine("TOTAL Created VM NUMBERS: ", broker.getVmsCreatedList().size(),"\n");
        Log.printConcatLine("ALl VMs Average Utilization: ", allVmsAverageCpuUtilization * 100,"%\n");
        Log.printConcatLine("------------------------ Static Experiment Parameters ------------------------\n");
        Log.printConcatLine("Packing_VM_SELECTION_TYPE: ", Parameters.Packing_VM_SELECTION_TYPE);
        Log.printConcatLine("R_T_Q_SCHEDULING_INTERVAL: ", Parameters.R_T_Q_SCHEDULING_INTERVAL);
        Log.printConcatLine("MONITORING_INTERVAL: ", Parameters.MONITORING_INTERVAL);
        Log.printConcatLine("CONTAINER_VM_SCHEDULING_INTERVAL: ", Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
        Log.printConcatLine("VM_THRESHOLD_FOR_SHUTDOWN: ", Parameters.VM_THRESH_HOLD_FOR_SHUTDOWN);
        Log.printConcatLine("CHECK_FINISHED_STATUS_DELAY: ", Parameters.CHECK_FINISHED_STATUS_DELAY, "\n");
        Log.printConcatLine("VM_PROVISIONING_DELAY: ", Parameters.VM_PROVISIONING_DELAY);
        Log.printConcatLine("VM_DESTROY_DELAY: ", Parameters.VM_DESTROY_DELAY);
        Log.printConcatLine("CONTAINER_PROVISIONING_DELAY: ", Parameters.CONTAINER_PROVISIONING_DELAY);
        Log.printConcatLine("CONTAINER_DESTROY_DELAY: ", Parameters.CONTAINER_DESTROY_DELAY, "\n");
        Log.printConcatLine("CPU_DEGRADATION: ", "MEAN: ", Parameters.CPU_DEGRADATION.getMean(), ", SD: ", Parameters.CPU_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BW_DEGRADATION: ", "MEAN: ", Parameters.BW_DEGRADATION.getMean(), ", SD: ", Parameters.BW_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BILLING_PERIOD: ", Parameters.BILLING_PERIOD);
        Log.printConcatLine("ALPHA_DEADLINE_FACTOR: ", Parameters.ALPHA_DEADLINE_FACTOR);
        Log.printConcatLine("BETA_BUDGET_FACTOR: ", Parameters.BETA_BUDGET_FACTOR);
        Log.printConcatLine("ARRIVAL_RATE: ", Parameters.ARRIVAL_RATE);
        Log.printConcatLine("-------------------------------------------------------------------\n");
    }
    private static void printStatus(Second_WorkflowEngine workflowEngine, WorkflowDatacenterBroker broker){
        int PSR = 0;
        int DeadlineSuccess = 0;
        double accumulatedWorkflowCost = 0;
        double accumulatedWorkflowBudget = 0;
        double accumulatedCostToBudget = 0;
        double accumulatedMakeSpanToDeadline = 0;
        int totalVmNumbers = broker.getVmDestroyedNumber() + broker.getVmsCreatedList().size();
        double totalVmCost = 0;
        double createdVmsAverageCpuUtilization = 0.0;
        double allVmsAverageCpuUtilization = 0.0;

        int totalWorkflowNumber = workflowEngine.getExecutedWorkflowList().size();

        for (Workflow workflow: workflowEngine.getExecutedWorkflowList()) {
            Log.printLine("--------------"+ workflow.getName()+ "-------------------");
            Log.printConcatLine("Deadline:", workflow.getDeadline(), "\nMake Span:", workflow.getFinalMakeSpan() + "\n");
            Log.printConcatLine("Budget:", workflow.getBudget(), "\nCost:", workflow.getTotalCost() + "\n");
            accumulatedWorkflowCost +=  workflow.getTotalCost();
            accumulatedWorkflowBudget += workflow.getBudget();
            accumulatedCostToBudget += workflow.getTotalCost() / workflow.getBudget();
            accumulatedMakeSpanToDeadline += workflow.getFinalMakeSpan() / workflow.getDeadline();

            if (workflow.getFinalMakeSpan() <= workflow.getDeadline() && workflow.getTotalCost() <= workflow.getBudget()){
                PSR++;
            }
            if (workflow.getFinalMakeSpan() <= workflow.getDeadline()){
                DeadlineSuccess++;
            }

        }
        for (ContainerVm vm : broker.getVmsCreatedList()){
            CondorVM castedVm = (CondorVM) vm;
            createdVmsAverageCpuUtilization += castedVm.getAverageCpuUtilization();
        }
        allVmsAverageCpuUtilization = (broker.getDestroyedVmAverageUtilization() * broker.getVmDestroyedNumber() + createdVmsAverageCpuUtilization) / totalVmNumbers;

        Log.enable();
        Log.printConcatLine("------------------------ Simulation Results ------------------------\n");
        Log.printConcatLine("Total workflows: ", workflowEngine.getWorkflowParser().getTotalWorkflowNumbers());
        Log.printConcatLine("PSR: ", (double)PSR *100 / totalWorkflowNumber, "%");
        Log.printConcatLine("Deadline success:  ", (double)DeadlineSuccess  * 100 / totalWorkflowNumber , "%");
        Log.printConcatLine("Accumulated Workflow Cost: ", accumulatedWorkflowCost);
        Log.printConcatLine("Average Workflow Cost: ", accumulatedWorkflowCost / totalWorkflowNumber);
        Log.printConcatLine("Accumulated Workflow Budget: ", accumulatedWorkflowBudget);
        Log.printConcatLine("Average Workflow Budget: ", accumulatedWorkflowBudget / totalWorkflowNumber);
        Log.printConcatLine("Average Cost to Budget:    ", (accumulatedCostToBudget / totalWorkflowNumber));
        Log.printConcatLine("Average MakeSpan to Deadline:  ", (accumulatedMakeSpanToDeadline / totalWorkflowNumber));
        Log.printConcatLine("TOTAL VM NUMBERS:  ", totalVmNumbers);
        Log.printConcatLine("TOTAL Destroyed VM NUMBERS: ", broker.getVmDestroyedNumber());
        Log.printConcatLine("TOTAL Created VM NUMBERS: ", broker.getVmsCreatedList().size(),"\n");
        Log.printConcatLine("ALl VMs Average Utilization: ", allVmsAverageCpuUtilization * 100,"%\n");
        Log.printConcatLine("------------------------ Static Experiment Parameters ------------------------\n");
        Log.printConcatLine("Packing_VM_SELECTION_TYPE: ", Parameters.Packing_VM_SELECTION_TYPE);
        Log.printConcatLine("R_T_Q_SCHEDULING_INTERVAL: ", Parameters.R_T_Q_SCHEDULING_INTERVAL);
        Log.printConcatLine("MONITORING_INTERVAL: ", Parameters.MONITORING_INTERVAL);
        Log.printConcatLine("CONTAINER_VM_SCHEDULING_INTERVAL: ", Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
        Log.printConcatLine("VM_THRESHOLD_FOR_SHUTDOWN: ", Parameters.VM_THRESH_HOLD_FOR_SHUTDOWN);
        Log.printConcatLine("CHECK_FINISHED_STATUS_DELAY: ", Parameters.CHECK_FINISHED_STATUS_DELAY, "\n");
        Log.printConcatLine("VM_PROVISIONING_DELAY: ", Parameters.VM_PROVISIONING_DELAY);
        Log.printConcatLine("VM_DESTROY_DELAY: ", Parameters.VM_DESTROY_DELAY);
        Log.printConcatLine("CONTAINER_PROVISIONING_DELAY: ", Parameters.CONTAINER_PROVISIONING_DELAY);
        Log.printConcatLine("CONTAINER_DESTROY_DELAY: ", Parameters.CONTAINER_DESTROY_DELAY, "\n");
        Log.printConcatLine("CPU_DEGRADATION: ", "MEAN: ", Parameters.CPU_DEGRADATION.getMean(), ", SD: ", Parameters.CPU_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BW_DEGRADATION: ", "MEAN: ", Parameters.BW_DEGRADATION.getMean(), ", SD: ", Parameters.BW_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BILLING_PERIOD: ", Parameters.BILLING_PERIOD);
        Log.printConcatLine("ALPHA_DEADLINE_FACTOR: ", Parameters.ALPHA_DEADLINE_FACTOR);
        Log.printConcatLine("BETA_BUDGET_FACTOR: ", Parameters.BETA_BUDGET_FACTOR);
        Log.printConcatLine("ARRIVAL_RATE: ", Parameters.ARRIVAL_RATE);
        Log.printConcatLine("-------------------------------------------------------------------\n");
    }
    private static void printStatus(MHHBDCS_WorkflowEngine workflowEngine, WorkflowDatacenterBroker broker){
        int PSR = 0;
        int DeadlineSuccess = 0;
        double accumulatedWorkflowCost = 0;
        double accumulatedWorkflowBudget = 0;
        double accumulatedCostToBudget = 0;
        double accumulatedMakeSpanToDeadline = 0;
        int totalVmNumbers = broker.getVmDestroyedNumber() + broker.getVmsCreatedList().size();
        double totalVmCost = 0;
        double createdVmsAverageCpuUtilization = 0.0;
        double allVmsAverageCpuUtilization = 0.0;

        int totalWorkflowNumber = workflowEngine.getExecutedWorkflowList().size();

        for (Workflow workflow: workflowEngine.getExecutedWorkflowList()) {
            Log.printLine("--------------"+ workflow.getName()+ "-------------------");
            Log.printConcatLine("Deadline:", workflow.getDeadline(), "\nMake Span:", workflow.getFinalMakeSpan() + "\n");
            Log.printConcatLine("Budget:", workflow.getBudget(), "\nCost:", workflow.getTotalCost() + "\n");
            accumulatedWorkflowCost +=  workflow.getTotalCost();
            accumulatedWorkflowBudget += workflow.getBudget();
            accumulatedCostToBudget += workflow.getTotalCost() / workflow.getBudget();
            accumulatedMakeSpanToDeadline += workflow.getFinalMakeSpan() / workflow.getDeadline();

            if (workflow.getFinalMakeSpan() <= workflow.getDeadline() && workflow.getTotalCost() <= workflow.getBudget()){
                PSR++;
            }
            if (workflow.getFinalMakeSpan() <= workflow.getDeadline()){
                DeadlineSuccess++;
            }

        }
        for (ContainerVm vm : broker.getVmsCreatedList()){
            CondorVM castedVm = (CondorVM) vm;
            createdVmsAverageCpuUtilization += castedVm.getAverageCpuUtilization();
        }
        allVmsAverageCpuUtilization = (broker.getDestroyedVmAverageUtilization() * broker.getVmDestroyedNumber() + createdVmsAverageCpuUtilization) / totalVmNumbers;

        Log.enable();
        Log.printConcatLine("------------------------ Simulation Results ------------------------\n");
        Log.printConcatLine("Total workflows: ", workflowEngine.getWorkflowParser().getTotalWorkflowNumbers());
        Log.printConcatLine("PSR: ", (double)PSR *100 / totalWorkflowNumber, "%");
        Log.printConcatLine("Deadline success:  ", (double)DeadlineSuccess  * 100 / totalWorkflowNumber , "%");
        Log.printConcatLine("Accumulated Workflow Cost: ", accumulatedWorkflowCost);
        Log.printConcatLine("Average Workflow Cost: ", accumulatedWorkflowCost / totalWorkflowNumber);
        Log.printConcatLine("Accumulated Workflow Budget: ", accumulatedWorkflowBudget);
        Log.printConcatLine("Average Workflow Budget: ", accumulatedWorkflowBudget / totalWorkflowNumber);
        Log.printConcatLine("Average Cost to Budget:    ", (accumulatedCostToBudget / totalWorkflowNumber));
        Log.printConcatLine("Average MakeSpan to Deadline:  ", (accumulatedMakeSpanToDeadline / totalWorkflowNumber));
        Log.printConcatLine("TOTAL VM NUMBERS:  ", totalVmNumbers);
        Log.printConcatLine("TOTAL Destroyed VM NUMBERS: ", broker.getVmDestroyedNumber());
        Log.printConcatLine("TOTAL Created VM NUMBERS: ", broker.getVmsCreatedList().size(),"\n");
        Log.printConcatLine("ALl VMs Average Utilization: ", allVmsAverageCpuUtilization * 100,"%\n");
        Log.printConcatLine("------------------------ Static Experiment Parameters ------------------------\n");
        Log.printConcatLine("Packing_VM_SELECTION_TYPE: ", Parameters.Packing_VM_SELECTION_TYPE);
        Log.printConcatLine("R_T_Q_SCHEDULING_INTERVAL: ", Parameters.R_T_Q_SCHEDULING_INTERVAL);
        Log.printConcatLine("MONITORING_INTERVAL: ", Parameters.MONITORING_INTERVAL);
        Log.printConcatLine("CONTAINER_VM_SCHEDULING_INTERVAL: ", Parameters.CONTAINER_VM_SCHEDULING_INTERVAL);
        Log.printConcatLine("VM_THRESHOLD_FOR_SHUTDOWN: ", Parameters.VM_THRESH_HOLD_FOR_SHUTDOWN);
        Log.printConcatLine("CHECK_FINISHED_STATUS_DELAY: ", Parameters.CHECK_FINISHED_STATUS_DELAY, "\n");
        Log.printConcatLine("VM_PROVISIONING_DELAY: ", Parameters.VM_PROVISIONING_DELAY);
        Log.printConcatLine("VM_DESTROY_DELAY: ", Parameters.VM_DESTROY_DELAY);
        Log.printConcatLine("CONTAINER_PROVISIONING_DELAY: ", Parameters.CONTAINER_PROVISIONING_DELAY);
        Log.printConcatLine("CONTAINER_DESTROY_DELAY: ", Parameters.CONTAINER_DESTROY_DELAY, "\n");
        Log.printConcatLine("CPU_DEGRADATION: ", "MEAN: ", Parameters.CPU_DEGRADATION.getMean(), ", SD: ", Parameters.CPU_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BW_DEGRADATION: ", "MEAN: ", Parameters.BW_DEGRADATION.getMean(), ", SD: ", Parameters.BW_DEGRADATION.getStandardDeviation());
        Log.printConcatLine("BILLING_PERIOD: ", Parameters.BILLING_PERIOD);
        Log.printConcatLine("ALPHA_DEADLINE_FACTOR: ", Parameters.ALPHA_DEADLINE_FACTOR);
        Log.printConcatLine("BETA_BUDGET_FACTOR: ", Parameters.BETA_BUDGET_FACTOR);
        Log.printConcatLine("ARRIVAL_RATE: ", Parameters.ARRIVAL_RATE);
        Log.printConcatLine("-------------------------------------------------------------------\n");
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
                    new ContainerVmBwProvisionerSimple(Parameters.HOST_BW),
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
                containerAllocationPolicy, new LinkedList<Storage>(), 50, experimentName, logAddress);

        return datacenter;
    }


}
