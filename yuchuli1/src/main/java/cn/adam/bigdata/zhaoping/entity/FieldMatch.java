package cn.adam.bigdata.zhaoping.entity;

import lombok.Getter;

public class   FieldMatch {
    private static final String ALL = "[\\x{4e00}-\\x{9fa5}|0-9|A-z]+";
    private static final String EMPTY = "\\s+";

    private static final String CFS = "^(A轮|B轮|C轮|D轮及以上|上市公司|不需要融资|天使轮|未融资)$";
    private static final String CI = "^(" + ALL + "[,|/]?" + ")+$";
    private static final String CL = null;
    private static final String CN = null;
    private static final String CNA = null;
    private static final String CO = null;
    private static final String CP = "^(公司规模：)?(\\d+人?(-|以上)?){1,2}$";
    private static final String JEDR = "^(学历：?)?((统招)?(MBA|不限|博士|博士后|大专|本科|" +
            "硕士|中专|中技|初中)+(及以下|及以上)?/*)+$";
    private static final String JEXR = "^(经验：?)*(不限|应届(毕业)?生|" +
            "(\\d+[年|-](以上|以下|以内)?)+)$";
    private static final String JI = null;
    private static final String JN = null;
    private static final String JS = "^((\\d+[k|K|万]?-?)+|面议)$";
    private static final String JT = null;
    private static final String JW = null;

    public static String[] getAllFieldMatch(){
        return new String[]{
                CFS,CI,CL,CN,CNA,CO,CP,JEDR,JEXR,JI,JN,JS,JT,JW
        };
    }

    public static String getALL() {
        return ALL;
    }

    public static String getEMPTY() {
        return EMPTY;
    }

    public static String getCFS() {
        return CFS;
    }

    public static String getCI() {
        return CI;
    }

    public static String getCL() {
        return CL;
    }

    public static String getCN() {
        return CN;
    }

    public static String getCNA() {
        return CNA;
    }

    public static String getCO() {
        return CO;
    }

    public static String getCP() {
        return CP;
    }

    public static String getJEDR() {
        return JEDR;
    }

    public static String getJEXR() {
        return JEXR;
    }

    public static String getJI() {
        return JI;
    }

    public static String getJN() {
        return JN;
    }

    public static String getJS() {
        return JS;
    }

    public static String getJT() {
        return JT;
    }

    public static String getJW() {
        return JW;
    }
}
