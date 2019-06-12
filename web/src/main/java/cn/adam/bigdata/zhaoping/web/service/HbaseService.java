package cn.adam.bigdata.zhaoping.web.service;

import cn.adam.bigdata.zhaoping.web.entity.CountValuve;
import cn.adam.bigdata.zhaoping.web.entity.KValuve;
import cn.adam.bigdata.zhaoping.web.util.HbaseUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class HbaseService {
    HbaseUtils hbase;
    ObjectMapper mapper = new ObjectMapper();
    public List<CountValuve> getCount(String n){
        try {
            ResultScanner job = hbase.tableScan("job", null);
            List<CountValuve> list = new ArrayList<>();
            for (Result r : job) {
                Cell columnLatestCell = r.getColumnLatestCell(Bytes.toBytes("count"), Bytes.toBytes(n));
                String row = Bytes.toString(CellUtil.cloneRow(columnLatestCell));
                list.add(new CountValuve(row, Integer.parseInt(Bytes.toString(CellUtil.cloneValue(columnLatestCell)))));
            }
            return list;
        } catch (Exception e) {
            log.error("查询出错！",e);
        }
        return null;
    }
    public String getString(String location, String cf, String cn){
        try {
            Result result = hbase.get("job", location, cf, cn);
            Cell[] cells = result.rawCells();
            for (int i = 0; i < cells.length; i++) {
                String s = Bytes.toString(CellUtil.cloneValue(cells[i]));
                return s;
            }
        }catch (Exception e) {
            log.error("查询出错！",e);
        }
        return null;
    }

    public List<KValuve> getGroup(String location, String cf) {
        try {
            Result result = hbase.get("job", location, cf);
            Cell[] cells = result.rawCells();
            List<KValuve> list = new ArrayList<>();
            for (int i = 0; i < cells.length; i++) {
                String cn = Bytes.toString(CellUtil.cloneQualifier(cells[i]));
                String value = Bytes.toString(CellUtil.cloneValue(cells[i]));
                List<CountValuve> map = mapper.readValue(value, new TypeReference<List<CountValuve>>(){});
//                JsonNode jsonNode = mapper.readTree(value);
                list.add(new KValuve(cn, map));
            }
            return list;

        } catch (Exception e) {
            log.error("查询出错！", e);
        }
        return null;
    }

    @PostConstruct
    public void init(){
        hbase = HbaseUtils.getInstance();
        log.info("初始化!");
    }
    @PreDestroy
    public void destroy(){
        hbase.close();
        log.info("销毁!");
    }
}
