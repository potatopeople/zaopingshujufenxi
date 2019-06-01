package cn.adam.bigdata.zhaoping.entity;

import org.apache.commons.csv.CSVFormat;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Scanner;

public class CSVFormats {
    public static CSVFormat FIRSTRECORDASHEADER = CSVFormat.DEFAULT.withFirstRecordAsHeader();
    public static CSVFormat BEFOR = null;
    public static CSVFormat AFTER = null;
    public static CSVFormat DEFAULT = CSVFormat.DEFAULT;

    static {
        try (
                Scanner beforsc = new Scanner(new File(CSVFormats.class.getResource("beforhandle.txt").getPath()));
                Scanner aftersc = new Scanner(new File(CSVFormats.class.getResource("afterhandle.txt").getPath()));
        ){
            StringBuilder bsb = new StringBuilder();
            StringBuilder asb = new StringBuilder();
            while (beforsc.hasNextLine())
                bsb.append(beforsc.nextLine()).append("\n");
            bsb.deleteCharAt(bsb.length()-1);
            BEFOR = CSVFormat.DEFAULT.withHeader(bsb.toString().split("\n"));

            while (aftersc.hasNextLine())
                asb.append(aftersc.nextLine()).append("\n");
            asb.deleteCharAt(asb.length()-1);
            AFTER = CSVFormat.DEFAULT.withHeader(asb.toString().split("\n"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
