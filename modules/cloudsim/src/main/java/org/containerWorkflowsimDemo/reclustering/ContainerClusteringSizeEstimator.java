package org.containerWorkflowsimDemo.reclustering;

public class ContainerClusteringSizeEstimator {
    /**
     * Here we assume n/k >> r
     *
     * @param k clustering size
     * @param t task runtime
     * @param s system overhead
     * @param theta parameter in estimating inter-arrival time
     * @param phi_gamma
     * @param phi_ts
     * @return the makespan
     */

    protected static double f(double k, double t, double s, double theta, double phi_gamma, double phi_ts) {
        double d = (k * t + s) * (phi_ts - 1);
        return d / k * Math.exp(Math.pow(d / theta, phi_gamma));
    }

    protected static double fprime(double k, double t, double s, double theta, double phi) {
        double first_part = Math.exp(Math.pow((k * t + s) / theta, phi));
        double second_part = t * phi / k * Math.pow((k * t + s) / theta, phi) - s / (k * k);
        return first_part * second_part;
    }

    public static int estimateK(double t, double s, double theta, double phi_gamma, double phi_ts) {
        int optimalK = 0;
        double minM = Double.MAX_VALUE;
        for (int k = 1; k < 200; k++) {
            double M = f(k, t, s, theta, phi_gamma, phi_ts);
            if (M < minM) {
                minM = M;
                optimalK = k;
            }
            //Log.printLine("k:" + k + " M: " + M);
        }
        return optimalK;
    }
}
