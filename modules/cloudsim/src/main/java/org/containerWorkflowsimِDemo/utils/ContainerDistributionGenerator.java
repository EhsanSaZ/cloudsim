package org.containerWorkflowsimِDemo.utils;

import org.apache.commons.math3.distribution.*;

import java.util.Arrays;

public class ContainerDistributionGenerator {
    protected ContainerDistributionGenerator.DistributionFamily dist;
    protected double scale;
    protected double shape;
    protected double scale_prior;
    protected double shape_prior;
    protected double likelihood_prior;
    protected double[] samples;
    protected double[] cumulativeSamples;
    protected int cursor;
    protected final int SAMPLE_SIZE = 1500 ; //DistributionGenerator will automatically increase the size


    public enum DistributionFamily {

        LOGNORMAL, GAMMA, WEIBULL, NORMAL
    }

    public ContainerDistributionGenerator(DistributionFamily dist, double scale, double shape) {
        this.dist = dist;
        this.scale = scale;
        this.shape = shape;
        this.scale_prior = scale;
        this.shape_prior = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        updateCumulativeSamples();
        cursor = 0;
    }

    public ContainerDistributionGenerator(DistributionFamily dist, double scale, double shape, double a, double b, double c) {
        this(dist, scale, shape);
        this.scale_prior = b;
        this.shape_prior = a;
        this.likelihood_prior = c;
    }

    public double[] getSamples() {
        return samples;
    }

    public double[] getCumulativeSamples() {
        return cumulativeSamples;
    }

    public void extendSamples() {
        double[] new_samples = getDistribution(scale, shape).sample(SAMPLE_SIZE);
        samples = concat(samples, new_samples);
        updateCumulativeSamples();
    }

    public void updateCumulativeSamples() {
        cumulativeSamples = new double[samples.length];
        cumulativeSamples[0] = samples[0];
        for (int i = 1; i < samples.length; i++) {
            cumulativeSamples[i] = cumulativeSamples[i - 1] + samples[i];
        }
    }


    public double getPKEMean() {
        return this.shape_prior / this.scale_prior;
    }


    public double getMean() {
        double sum = 0.0;
        for (int i = 0; i < cursor; i++) {
            sum += samples[i];
        }
        return sum / cursor;
    }


    public double getLikelihoodPrior() {
        return this.likelihood_prior;
    }


    public double getMLEMean() {
        double a = shape_prior, b = scale_prior;
        double sum = 0.0;

        for (int i = 0; i < cursor; i++) {
            switch (dist) {
                case GAMMA:
                    sum += samples[i];
                    break;
                case WEIBULL:
                    sum += Math.pow(samples[i], likelihood_prior);
                    break;
            }
        }
        double result = 0.0;
        switch (dist) {
            case GAMMA:
                result = (b + sum) / (a + cursor * likelihood_prior - 1);
                break;
            case WEIBULL:
                result = (b + sum) / (a + cursor + 1);
                break;
            default:
                break;
        }
        return result;
    }

    public void varyDistribution(double scale, double shape) {
        this.scale = scale;
        this.shape = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        updateCumulativeSamples();
        //cursor = 0;
    }

    public double[] concat(double[] first, double[] second) {
        double[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }


    public double getNextSample() {
        while (cursor >= samples.length) {
            double[] new_samples = getDistribution(scale, shape).sample(SAMPLE_SIZE);
            samples = concat(samples, new_samples);
            updateCumulativeSamples();
        }
        double delay = samples[cursor];
        cursor++;
        return delay;
    }

    public RealDistribution getDistribution(double scale, double shape) {
        RealDistribution distribution = null;
        switch (this.dist) {
            case LOGNORMAL:
                distribution = new LogNormalDistribution(scale, shape);
                break;
            case WEIBULL:
                distribution = new WeibullDistribution(shape, scale);
                break;
            case GAMMA:
                distribution = new GammaDistribution(shape, scale);
                break;
            case NORMAL:
                //shape is the std, scale is the mean
                distribution = new NormalDistribution(scale, shape);
                break;
            default:
                break;
        }
        return distribution;
    }

    public double getScale()
    {
        return this.scale;
    }

    public double getShape(){
        return this.shape;
    }
}
