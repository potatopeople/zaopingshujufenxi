package cn.adam.bigdata.zhaoping.handle;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class Correction extends HandleTemp<List<String>> {
    @Override
    public void handle(List<String> list) {
        cPeopleCorr(list);
        cLocaltionCorr(list);
    }

    private void cPeopleCorr(List<String> list){
        Pattern p = Pattern.compile(FieldMatch.CP);
        Matcher matcher = p.matcher(list.get(0));
        if (matcher.matches()){
            if ((list.get(1) != null) && list.get(1).equals(list.get(6)))
                list.set(6, list.get(0));
            else {
                String s1 = list.get(1);
                list.set(1, "tmp");
                int i = find(list, s1);
                if (i > 1 && i < 10)
                    list.set(i, list.get(0));
                else
                    log.warn("人数错误，但未找到其对应的放置位置："+list.get(3));
            }
            list.set(0, "");
        }
    }

    private void cLocaltionCorr(List<String> list){
        Pattern p = Pattern.compile(FieldMatch.CFS);
        Matcher matcher = p.matcher(list.get(0));

        if (!matcher.matches()){
            if ((list.get(2) == null) || "".equals(list.get(2)))
                list.set(2, list.get(0));

            list.set(0, "");
        }
    }

    private int find(List<String> list, String s){
        return Collections.binarySearch(list, s, (o1, o2) -> {
            int i = o1.compareTo(o2);
            if (i == 0)
                return 0;
            return Pattern.compile(o2).matcher(o1).matches() ? 0 : i;
        });
    }
}
