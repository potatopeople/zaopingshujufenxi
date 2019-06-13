package cn.adam.bigdata.zhaoping;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.handle.Correction;
import cn.adam.bigdata.zhaoping.handle.Filter;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Scanner;

@Slf4j
public class CsvFormat {

    static {
        log.info("初始化!");
        try {
//            URL url = CsvFormat.class.getClassLoader().getResource(".");
            Scanner sc = new Scanner(System.in);
            System.out.println("输入文件夹地址：");
            String s = sc.nextLine();
//            assert url != null;
//            File f = new File(url.toURI());
            File f = new File(s);
            URL url = f.toURI().toURL();

            files = f.listFiles((dir, name) -> name.startsWith("job") && name.endsWith(".csv"));

            out = new File(url.getPath()+"out/ja.csv");

            handles = new Handle[]{
                    new Filter(),
                    new Correction()
            };
        } catch (Exception e) {
            log.error("初始化时出错！", e);
            files = new File[0];
        }
    }

    private static File[] files;
    private static File out;
    private static Handle[] handles;

    public static void main(String[] args) {
        log.info("开始处理!");
        log.debug(Utils.getAll(files, "getName", ",", false).toString());

        CsvFormat csv = new CsvFormat();

        if (!out.exists()){
            try {
                File outDir = out.getParentFile();
                if (!outDir.exists()) {
                    outDir.mkdir();
                }
                out.createNewFile();
            } catch (IOException e) {
                log.error("创建输出文件失败!", e);
                return;
            }
        }
        try (Writer writer = new PrintWriter(out, "UTF-8")){
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
            for (File f : files) {
                csv.work(f, printer);
            }

            log.info("处理完成，输出文件："+out.getPath());
        } catch (IOException e) {
            log.error("输错流出错！", e);
        }
    }

    private boolean first = true;
    private void work(File f, CSVPrinter printer){
        log.info("处理文件: " + f.getName());
        try  {
            CSVParser csvParser = CSVParser.parse(f, Charset.forName("UTF-8"),CSVFormat.DEFAULT);

            for (CSVRecord record : csvParser) {
                List<String> list = Utils.csvstrRecordToList(record);

                if (first){
                    printer.printRecord(list);
                    first = false;
                    continue;
                }
                if ("company_financing_stage".equals(list.get(0)))
                    continue;

                for (Handle handle : handles) {
                    handle.handle(list);
                }

                printer.printRecord(list);
            }
        } catch (IOException e) {
            log.error("读取" + f.getName() + "文件出错！", e);
        }
    }
}
