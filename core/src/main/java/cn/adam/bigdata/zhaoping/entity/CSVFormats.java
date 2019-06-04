package cn.adam.bigdata.zhaoping.entity;

import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@Slf4j
public class CSVFormats extends HaveConfFileTemp{
    private static CSVFormat FIRSTRECORDASHEADER = CSVFormat.DEFAULT.withFirstRecordAsHeader();
    private static CSVFormat DEFAULT = CSVFormat.DEFAULT;
    private static CSVFormat  BEFOR = null;
    private static CSVFormat AFTER = null;
    private static CSVFormat AFTERGETLOCATION = null;
//    public static CSVFormat  BEFOR = CSVFormat.DEFAULT.withHeader(
//            "company_financing_stage", "company_industry", "company_location",
//            "company_name", "company_nature", "company_overview", "company_people",
//            "job_edu_require", "job_exp_require", "job_info", "job_name", "job_salary",
//            "job_tag", "job_welfare"
//    );
//    public static CSVFormat AFTER = CSVFormat.DEFAULT.withHeader(
//            "company_financing_stage","company_industry","company_location",
//            "company_name", "company_nature", "company_overview", "company_people",
//            "company_people_handle", "company_people_level", "job_edu_require",
//            "job_edu_require_handle", "job_edu_require_level", "job_exp_require",
//            "job_exp_require_handle", "job_exp_require_level", "job_info", "job_info_words",
//            "job_name", "job_salary", "job_salary_handle", "job_salary_level", "job_tag",
//            "job_welfare", "job_welfare_words"
//    );


    private synchronized static void setCSVFormat() {
        if (!Utils.haveNull(BEFOR, AFTER, AFTERGETLOCATION))
            return;

        InputStream bhin = null;
        InputStream ahin = null;
        InputStream aglin = null;
        if (confPath != null) {
            try {
                FileSystem fs = FileSystem.get(CONF);
                FileStatus[] fileStatuses = fs.listStatus(confPath);
                for (FileStatus f : fileStatuses) {
                    Path path = f.getPath();
                    String fn = path.getName();

                    if ("beforhandle.txt".equals(fn))
                        bhin = fs.open(path);
                    else if ("afterhandle.txt".equals(fn))
                        ahin = fs.open(path);
                    else if ("aftergetlocation.txt".equals(fn))
                        aglin = fs.open(path);
                }
            }catch (Exception e){
                log.error("初始化CSVFormats获取字段文件出错！", e);
                throw new RuntimeException(e);
            }
        }else {
            try {
                bhin = new FileInputStream(new File(CSVFormats.class.getResource("beforhandle.txt").toURI()));
                ahin = new FileInputStream(new File(CSVFormats.class.getResource("afterhandle.txt").toURI()));
                aglin = new FileInputStream(new File(CSVFormats.class.getResource("aftergetlocation.txt").toURI()));
            } catch (Exception e) {
                log.error("初始化自定义词库获取词库文件出错！", e);
                throw new RuntimeException(e);
            }
        }

        try (
                Scanner bhsc = new Scanner(bhin);
                Scanner ahsc = new Scanner(ahin);
                Scanner aglsc = new Scanner(aglin)
        ) {
            List<String> list = new ArrayList<>();
            while (bhsc.hasNextLine()) {
                String s = bhsc.nextLine();
                if (s == null || s.equals("")) {
                    continue;
                }
                list.add(s);
            }
            BEFOR = CSVFormat.DEFAULT.withHeader(list.toArray(new String[list.size()]));
            list.clear();

            while (ahsc.hasNextLine()) {
                String s = ahsc.nextLine();
                if (s == null || s.equals("")) {
                    continue;
                }
                list.add(s);
            }
            AFTER = CSVFormat.DEFAULT.withHeader(list.toArray(new String[list.size()]));
            list.clear();

            while (aglsc.hasNextLine()) {
                String s = aglsc.nextLine();
                if (s == null || s.equals("")) {
                    continue;
                }
                list.add(s);
            }
            AFTERGETLOCATION = CSVFormat.DEFAULT.withHeader(list.toArray(new String[list.size()]));
            log.info(BEFOR.toString());
            log.info(AFTER.toString());
            log.info(AFTERGETLOCATION.toString());

        }catch (Exception e){
            log.error("初始化自定义词库读取词库文件出错！", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public String[] getConfFile() {
        return new String[]{
                CSVFormats.class.getResource("beforhandle.txt").getPath(),
                CSVFormats.class.getResource("afterhandle.txt").getPath(),
                CSVFormats.class.getResource("aftergetlocation.txt").getPath()
        };
    }

    public static CSVFormat getFIRSTRECORDASHEADER() {
        return FIRSTRECORDASHEADER;
    }

    public static CSVFormat getDEFAULT() {
        return DEFAULT;
    }

    public static CSVFormat getBEFOR() {
        if (BEFOR == null) setCSVFormat();
        return BEFOR;
    }

    public static CSVFormat getAFTER() {
        if (AFTER == null) setCSVFormat();
        return AFTER;
    }

    public static CSVFormat getAFTERGETLOCATION() {
        if (AFTERGETLOCATION == null) setCSVFormat();
        return AFTERGETLOCATION;
    }
}
