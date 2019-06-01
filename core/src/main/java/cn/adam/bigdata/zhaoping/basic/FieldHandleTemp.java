package cn.adam.bigdata.zhaoping.basic;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class FieldHandleTemp<T> implements FieldHandle<T> {
    public FieldHandleTemp(){
        log.info(this.getClass().getName()+"初始化!");
    }
    @Override
    public abstract void handle(T t);
}
