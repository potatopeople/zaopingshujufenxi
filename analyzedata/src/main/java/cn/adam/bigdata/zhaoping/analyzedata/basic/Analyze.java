package cn.adam.bigdata.zhaoping.analyzedata.basic;

public interface Analyze<T, I> {
    void handle(T t, I i);
}
