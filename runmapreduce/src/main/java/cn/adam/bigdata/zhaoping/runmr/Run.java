package cn.adam.bigdata.zhaoping.runmr;

import java.util.Scanner;

public class Run {
    public static void main(String[] args) {
        System.out.println("（1）Handle");
        System.out.println("（2）Location");
        System.out.println("（3）Analyze");
        Scanner sc = new Scanner(System.in);
        switch (sc.nextInt()){
            case 1:
                RunHandleMapReduce.main(new String[0]);
                break;
            case 2:
                RunLocationMapReduce.main(new String[0]);
                break;
            case 3:
                RunAnalyzeMapReduce.main(new String[0]);
                break;
        }
    }
}
