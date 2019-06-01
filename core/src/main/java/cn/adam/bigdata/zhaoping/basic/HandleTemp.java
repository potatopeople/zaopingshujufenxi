package cn.adam.bigdata.zhaoping.basic;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class HandleTemp<T> implements Handle<T> {
    public HandleTemp(){
        log.info(this.getClass().getName()+"初始化!");
    }
    @Override
    public abstract void handle(T t);
}
