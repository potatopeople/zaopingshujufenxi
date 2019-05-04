package cn.adam.bigdata.zhaoping.handle;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;

import java.util.Arrays;
import java.util.List;

public class FilterField extends HandleTemp {

    private final int[] x = {0, 6, 7, 8, 11};

    @Override
    public void handle(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            boolean is = Arrays.binarySearch(x, i) >= 0;
            list.set(i, work(list.get(i), is));
        }
    }

    private String work(String s, boolean is) {
        return s.replaceAll(FieldMatch.getEMPTY(), is ? "" : " ")
                .replaceAll("(^ | $)", "");
    }
}
