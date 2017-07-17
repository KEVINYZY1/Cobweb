package com.xiongbeer.webveins;

/**
 * Created by shaoxiong on 17-4-9.
 */
public class Test {

    public static long numBits(long n, double p) {
        if (p == 0.0D) {
            p = 4.9E-324D;
        }
        return (long) ((double) (-n) * Math.log(p) / (Math.log(2.0D) * Math.log(2.0D)));
    }

    public static long getN(long numBits, double p) {
        return (long) ((double) (-numBits) * (Math.log(2.0D) * Math.log(2.0D)) / Math.log(p));
    }

    public static double getP(long numBits, long n) {
        return Math.exp(numBits * (Math.log(2.0D) * Math.log(2.0D)) / (-n));
    }

    public static void main(String[] args) {
        long n = 1000000;
        double p = 0.000001;
    }
}
