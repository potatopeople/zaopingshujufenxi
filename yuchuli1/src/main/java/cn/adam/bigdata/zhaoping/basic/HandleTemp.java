package cn.adam.bigdata.zhaoping.basic;

import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public abstract class HandleTemp implements Handle {
    @Override
    public abstract void handle(List<String> list);
}
