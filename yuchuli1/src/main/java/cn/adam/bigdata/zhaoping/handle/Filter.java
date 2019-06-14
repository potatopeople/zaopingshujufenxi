package cn.adam.bigdata.zhaoping.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.util.Utils;

import java.util.List;

public class Filter implements Handle<List<String>> {

    private final Integer[] x = {0, 6, 7, 8, 11};

    @Override
    public void handle(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            boolean is = Utils.binarySearch(x, i) >= 0;
            list.set(i, work(list.get(i), is));
        }
    }

    /**
     * 去除字段中多余空白符（空格，换行，制表符等）
     * @param s 字段字符串
     * @param is 指定是消除所有空白符还是，消除多余空白符
     * @return 返回处理后的字符串
     */
    private String work(String s, boolean is) {
        return s.replaceAll(FieldMatch.EMPTY, is ? "" : " ")
                .replaceAll("(^\\s+|\\s+$)", "");
    }
}
