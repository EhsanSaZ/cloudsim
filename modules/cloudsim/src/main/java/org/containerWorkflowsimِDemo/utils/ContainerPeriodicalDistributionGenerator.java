package org.containerWorkflowsimِDemo.utils;


import org.apache.commons.math3.distribution.RealDistribution;

public class ContainerPeriodicalDistributionGenerator extends ContainerDistributionGenerator{
    protected ContainerPeriodicalSignal signal;

    public ContainerPeriodicalDistributionGenerator(DistributionFamily dist, double scale, double shape, ContainerPeriodicalSignal signal){
        super(dist, scale, shape);
        this.signal = signal;
        //generate samples periodically
        double currentTime = 0.0;
        samples = generatePeriodicalSamples(currentTime);

        updateCumulativeSamples();
        cursor = 0;

    }

    public ContainerPeriodicalDistributionGenerator(DistributionFamily dist, double scale, double shape, double a, double b, double c, ContainerPeriodicalSignal signal){
        super(dist, scale, shape, a, b, c);
        this.signal = signal;
        double currentTime = 0.0;
        samples = generatePeriodicalSamples(currentTime);
        updateCumulativeSamples();
        cursor = 0;
    }

    @Override
    public void extendSamples() {
        double currentTime = cumulativeSamples[cumulativeSamples.length - 1];
        double[] new_samples = generatePeriodicalSamples(currentTime);
        samples = concat(samples, new_samples);
        updateCumulativeSamples();
    }

    /**
     * Generates a periodical sample
     * @return samples
     */
    private double[] generatePeriodicalSamples(double currentTime){
        RealDistribution distribution_upper = getDistribution(signal.getUpperBound(), shape);
        RealDistribution distribution_lower = getDistribution(signal.getLowerBound(), shape);
        RealDistribution distribution;
        double[] periodicalSamples = new double[SAMPLE_SIZE];
        boolean direction = signal.getDirection();
        for(int i = 0; i < SAMPLE_SIZE; i ++){
            if(currentTime % signal.getPeriod() < signal.getPeriod() * signal.getPortion()){
                if(direction){
                    distribution = distribution_upper;
                }else{
                    distribution = distribution_lower;
                }
            }else{
                if(direction){
                    distribution = distribution_lower;
                }else{
                    distribution = distribution_upper;
                }
            }
            periodicalSamples[i] = distribution.sample();
            currentTime += periodicalSamples[i];

        }
        return periodicalSamples;
    }
}
