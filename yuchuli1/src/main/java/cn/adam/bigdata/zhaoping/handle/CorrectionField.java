package cn.adam.bigdata.zhaoping.handle;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class CorrectionField extends HandleTemp {
    @Override
    public void handle(List<String> list) {

    }

    private void cPeopleCorr(List<String> list){
        Pattern p = Pattern.compile(FieldMatch.getCP());
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

    private void cPositionCorr(List<String> list){
        int cp = find(list, FieldMatch.getCP());
        int jedr = find(list, FieldMatch.getJEDR());
        int jexr = find(list, FieldMatch.getJEXR());
//        if (cp >= 0 && cp != 6 && ){
//
//        }
    }

    private int find(List<String> list, String s){
        return Collections.binarySearch(list, s, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int i = o1.compareTo(o2);
                if (i == 0)
                    return 0;
                return Pattern.compile(o2).matcher(o1).matches() ? 0 : i;
            }
        });
    }
}
