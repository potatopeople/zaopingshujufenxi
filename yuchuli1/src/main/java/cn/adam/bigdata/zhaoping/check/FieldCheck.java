package cn.adam.bigdata.zhaoping.check;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FieldCheck extends HandleTemp {
    @Override
    public void handle(List<String> list) {
        String[] all = FieldMatch.getAllFieldMatch();
        for (int i = 0; i < list.size(); i++) {
            String p = all[i];
            if (p == null)
                continue;
            Matcher matcher = Pattern.compile(p).matcher(list.get(i));
            if (!matcher.matches())
                log.warn((i+1)+"字段不符合！：\t" + list.get(i));
        }
    }
}
