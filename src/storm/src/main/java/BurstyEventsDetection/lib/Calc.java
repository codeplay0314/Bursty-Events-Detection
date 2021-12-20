package BurstyEventsDetection.lib;

public class Calc {

    public static double avg(Double[] A) {
        double sum = 0;
        for (Double a : A) {
            sum += a;
        }
        return sum / A.length;
    }

    public static double dev(Double[] A) {
        double avg = avg(A), sum = 0;
        for (Double a : A) {
            sum += (a - avg) * (a - avg);
        }
        return Math.sqrt(sum / A.length);
    }
}
