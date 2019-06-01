package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.FieldHandleTemp;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

@Slf4j
public class WordFieldHandle extends FieldHandleTemp<JobWritable> {

    private static final String IT = "userDefineIT";
    private static final String WEL = "userDefineWEL";
    private static final Map<String, String> ITWORDS = new HashMap<>();
    private static final Set<String> WELWORDS = new HashSet<>();
    static {
        File itfile = new File(WordFieldHandle.class.getResource("it.txt").getPath());
        File welfile = new File(WordFieldHandle.class.getResource("welfare.txt").getPath());
        try(
                Scanner itsc = new Scanner(itfile);
                Scanner welsc = new Scanner(welfile)
        ) {
            while (itsc.hasNextLine()){
                String s = itsc.nextLine();
                if (s == null || s.equals(""))
                    continue;
                String[] ss = s.split("\t");
                for (String word : ss[1].split("=")) {
                    ITWORDS.put(word, ss[0]);
                    DicLibrary.insert(DicLibrary.DEFAULT, word);
                    log.debug(word);
                }
            }
            while (welsc.hasNextLine()){
                String s = welsc.nextLine();
                if (s == null || s.equals(""))
                    continue;
                log.debug(s);
                WELWORDS.add(s);
                DicLibrary.insert(DicLibrary.DEFAULT, s);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void handle(JobWritable jobWritable) {
        String info = jobWritable.getJob_info()
                +jobWritable.getJob_name()+jobWritable.getJob_welfare();

        Result result = ToAnalysis.parse(info);
        Set<String> itset = new HashSet<>();
        Set<String> welset = new HashSet<>();

        for (Term t : result) {
            if (ITWORDS.containsKey(t.getName()))
                itset.add(ITWORDS.get(t.getName()));
            else if (WELWORDS.contains(t.getName()))
                welset.add(t.getName());
        }

        Object ito = Utils.getAll(itset, "toString", ",", false);
        Object welo = Utils.getAll(welset, "toString", ",", false);

        jobWritable.setJob_info_words(ito==null? (String) ito :ito.toString());
        jobWritable.setJob_welfare_words(welo==null? (String) welo :welo.toString());
    }

}
