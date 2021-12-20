package BurstyEventsDetection.lib;

public class BurstyProb {

    public static double calc(int N, int k, double p) {
        double L = (N + 1) * p;
        if (k <= L) return 0;
        int l = (int) Math.round(L), r = N;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (Binomial.binomial(N, mid, p) > 1e-13) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        double R = r;
        if (k > R) return 1;
        return 1.0 / (1 + Math.exp(-(k - Math.round((L + R) / 2))));
    }

}
