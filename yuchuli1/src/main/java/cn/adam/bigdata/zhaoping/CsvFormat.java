package cn.adam.bigdata.zhaoping;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.handle.CorrectionField;
import cn.adam.bigdata.zhaoping.handle.FilterField;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

@Slf4j
public class CsvFormat {

    static {
        log.info("初始化!");
        try {
            URL url = CsvFormat.class.getClassLoader().getResource(".");
            assert url != null;
            File f = new File(url.toURI());
            files = f.listFiles((dir, name) -> name.startsWith("job") && name.endsWith(".csv"));

            out = new File(url.getPath()+"out/ja.csv");

            handles = new Handle[]{
                    new FilterField(),
                    new CorrectionField()
            };
        } catch (URISyntaxException e) {
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
                    outDir.createNewFile()
                }
                out.createNewFile()
            } catch (IOException e) {
                log.error("创建输出文件失败!", e);
                return;
            }
        }
        try (Writer writer = new FileWriter(out)){
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
            for (File f : files) {
                csv.work(f, printer);
            }
        } catch (IOException e) {
            log.error("输错流出错！", e);
        }
    }

    private boolean first = true;
    private void work(File f, CSVPrinter printer){
        log.info("处理文件: " + f.getName());
        try (Reader reader = new FileReader(f)) {
            CSVParser csvParser = new CSVParser(reader,CSVFormat.DEFAULT);

            for (CSVRecord record : csvParser) {
                List<String> list = Utils.csvRecordToList(record);

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
