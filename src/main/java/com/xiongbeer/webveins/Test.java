package com.xiongbeer.webveins;

import com.google.common.primitives.UnsignedBytes;
import com.xiongbeer.webveins.filter.UrlFilter;
import com.xiongbeer.webveins.utils.MD5Maker;

import javax.swing.text.html.Option;
import java.io.*;
import java.util.*;

/**
 * Created by shaoxiong on 17-4-9.
 */
public class Test {

    public static long numBits(long n, double p){
        if(p == 0.0D) {
            p = 4.9E-324D;
        }

        return (long)((double)(-n) * Math.log(p) / (Math.log(2.0D) * Math.log(2.0D)));
    }

    public static long getN(long numBits, double p){
        return (long)((double)(-numBits) * (Math.log(2.0D) * Math.log(2.0D)) / Math.log(p));
    }

    public static double getP(long numBits, long n){
        return Math.exp(numBits * (Math.log(2.0D) * Math.log(2.0D)) / (-n));
    }

    public static void main(String[] args){
        long n = 1000000;
        double p = 0.000001;
        long numBits = (long)1 << 24;
        UrlFilter filter1 = new UrlFilter(n, getP(numBits, n));
        UrlFilter filter2 = new UrlFilter(n, getP(numBits - 10, n));
        List<String> list = new LinkedList<>();
        for(int i=0; i<5000000; i++){
            MD5Maker md5Maker = new MD5Maker(Integer.toString(i));
            list.add(md5Maker.toString());
        }
        long time1 = System.currentTimeMillis();
        for(String md5:list){
            filter1.put(md5);
        }
        long time2 = System.currentTimeMillis();
        for(String md5:list){
            filter2.put(md5);
        }
        long time3 = System.currentTimeMillis();
        for(String md5:list){
            filter2.mightContain(md5);
        }
        long time4 = System.currentTimeMillis();
        for(String md5:list){
            filter1.mightContain(md5);
        }
        long time5 = System.currentTimeMillis();
        System.out.println((time4-time3) + ":" + (time5-time4));
    }
}
