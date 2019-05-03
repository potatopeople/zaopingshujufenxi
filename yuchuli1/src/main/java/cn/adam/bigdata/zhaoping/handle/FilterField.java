package cn.adam.bigdata.zhaoping.handle;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;

import java.util.List;

public class FilterField extends HandleTemp {

    private final String all = "[^\\x{4e00}-\\x{9fa5}|0-9|A-z]+";
    private final String empty = "\\s+";

    @Override
    public void handle(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            list.set(i, work(list.get(i)));
        }
    }

    private String work(String s) {
        return s.replaceAll(empty, " ")
                .replaceAll("(^ | $)", "");
    }
}
