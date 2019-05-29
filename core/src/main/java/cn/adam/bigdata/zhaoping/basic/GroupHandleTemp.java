package cn.adam.bigdata.zhaoping.basic;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class GroupHandleTemp implements GroupHandle {
    public GroupHandleTemp(){
        log.info(this.getClass().getName()+"初始化!");
    }
    @Override
    public abstract void handle(List<List<String>> list);
}
