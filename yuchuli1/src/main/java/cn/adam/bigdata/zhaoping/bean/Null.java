package cn.adam.bigdata.zhaoping.bean;

import java.util.Objects;

public class Null {
    private Null(){}

    public static Null getInstance(){
        return NullTmp.nullTmp;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return Objects.isNull(obj);
    }
    @Override
    public int hashCode() {
        return 0;
    }

    private static class NullTmp {
        private static final Null nullTmp = new Null();
    }
}
