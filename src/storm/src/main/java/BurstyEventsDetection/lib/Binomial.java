package BurstyEventsDetection.lib;

public class Binomial {
    public static double combination(double N, double k) {
        double min = k;
        double max = N - k;
        double t = 0;

        double NN = 1;
        double kk = 1;

        if (min > max) {
            t = min;
            min = max;
            max = t;
        }
        while (N > max) {
            NN = NN * N;
            N--;
        }
        while (min > 0) {
            kk = kk * min;
            min--;
        }
        return NN / kk;
    }

    public static double binomial(int N, int k, double p) {
        double a = 1, b = 1;
        double c = combination(N, k);

        while ((N - k) > 0) {
            a = a * (1 - p);
            N--;
        }
        while (k > 0) {
            b = b * p;
            k--;
        }
        return c * a * b;
    }
}
