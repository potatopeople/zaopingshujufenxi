package cn.adam.bigdata.zhaoping.web.controller;

import cn.adam.bigdata.zhaoping.web.entity.CountValuve;
import cn.adam.bigdata.zhaoping.web.entity.KValuve;
import cn.adam.bigdata.zhaoping.web.service.HbaseService;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Getter
@Setter
public class DefaultController {
    @Autowired
    private HbaseService hbaseService;
    @GetMapping("/companycount")
    public List<CountValuve> getCompanyCount(){
        return hbaseService.getCount("company");
    }
    @GetMapping("/jobcount")
    public List<CountValuve> getJobCount(){
        return hbaseService.getCount("jobtag");
    }

    @GetMapping("/jobrank")
    public String getJobRank(String location){
        return hbaseService.getString(location, "rank", "jobtag");
    }
    @GetMapping("/naturerank")
    public String getNatureRank(String location){
        return hbaseService.getString(location, "rank", "nature");
    }
    @GetMapping("/industryrank")
    public String getIndustryRank(String location){
        return hbaseService.getString(location, "rank", "industry");
    }
    @GetMapping("/inforank")
    public String getInfoRank(String location){
        return hbaseService.getString(location, "rank", "info");
    }
    @GetMapping("/welfarerank")
    public String getWelfareRank(String location){
        return hbaseService.getString(location, "rank", "welfare");
    }
    @GetMapping("/eduradar")
    public String getEduRadar(String location){
        return hbaseService.getString(location, "radarchart", "edu");
    }
    @GetMapping("/expradar")
    public String getExpRadar(String location){
        return hbaseService.getString(location, "radarchart", "exp");
    }
    @GetMapping("/eesscatter")
    public String getEesScatter(String location){
        return hbaseService.getString(location, "scatterplot", "ees");
    }
    @GetMapping("/eesjscatter")
    public String getEesjScatter(String location){
        return hbaseService.getString(location, "scatterplot", "eesj");
    }


    @GetMapping("/info")
    public List<KValuve> getInfo(String location){
        return hbaseService.getGroup(location, "info");
    }
    @GetMapping("/welfare")
    public List<KValuve> getWelfare(String location){
        return hbaseService.getGroup(location, "welfare");
    }
}
