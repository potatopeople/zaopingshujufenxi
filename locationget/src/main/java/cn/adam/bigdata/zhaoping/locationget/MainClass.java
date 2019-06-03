package cn.adam.bigdata.zhaoping.locationget;

import cn.adam.bigdata.zhaoping.locationget.entity.Location;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.File;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Scanner;

public class MainClass {
    private static String[] tencentKey = {
            "VLABZ-TJNKF-326JS-NW6NB-H7Y6S-DJBVA",     //王恒兴
//            "7YCBZ-N2BWX-WJV44-ZSZES-REMDK-4WB6B",
            "UXRBZ-UUI6X-FJI4B-ZJABP-KPZDS-6EBQD",  //缑鑫
            "UA4BZ-YB3WP-PSADF-VFOYF-N2HP5-JDBVY"   //喵喵
//            "UHABZ-TBOWX-LJH4U-ZYUA3-6H5D6-MPBNJ"
    };


    private static String amapKey = "5aec35ad705af96d49970176e73317bf";
    public static void main(String[] args) {
        MainClass mainClass = new MainClass();

//        Location fromTencent1 = mainClass.getFromTencent("北京市海淀区东北旺西路8号院中关村软件园信息中心1#A306", 1);
//        System.out.println(fromTencent1);
//        if (true) return;

        File f = new File(MainClass.class.getClassLoader().getResource("location.txt").getPath());

        double i = 1;
        try (
                Scanner sc = new Scanner(f);
                PrintWriter p = new PrintWriter(new File("D:\\l.txt"));
                PrintWriter perr = new PrintWriter(new File("D:\\lerr.txt"))
        ){
            boolean is = true;
            while (sc.hasNextLine()){
                String s = sc.nextLine();

//                if (is){
//                    if (s.equals("高新区宝源路780号"))
//                        is = false;
//                    i+=1;
//                    continue;
//                }
                Location fromTencent = mainClass.getFromTencent(s, 1);
                if (fromTencent == null) {
                    perr.println(s);
                    perr.flush();
                    continue;
                }
                p.println(fromTencent.getProvince()+"\t"
                        +fromTencent.getCity()+"\t"
                        +fromTencent.getDistrict()+"\t"
                        +fromTencent.getLongitude()+"\t"
                        +fromTencent.getLatitude()+"\t"
                        +fromTencent.getLocation()
                );

                double tmp = i/18251.0*100.0;
                if (i%100 == 0) {
                    p.flush();
                    System.out.println(tmp+"%");
                }
                i+=1;
                try {
                    Thread.sleep(50);
                }catch (Exception e){}
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private int i = 0;
    private Location getFromTencent(String location, int x){
        if (i >= tencentKey.length) {
            i = 0;
        }
        String key = tencentKey[i];
        i++;

        JSONObject jsonObject = null;
        try {
            jsonObject = getJson("https://apis.map.qq.com/ws/geocoder/v1/?address="+
                    URLEncoder.encode(location, "UTF-8")
                    +"&key="
                    +key);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Integer status = jsonObject.getInteger("status");
        if (status == null) status = -1;
        if (status == -1 && x <2) {
            return getFromTencent(location, x + 1);
        }else if (x >= 2||(status>=300&&status<=400))
            return getFromAmap(location, 1);
        if (status != 0 && status != 101)
            System.err.println("Key失效:"+key);
        else if (status == 0){
            JSONObject result = jsonObject.getJSONObject("result");
            if (result!=null) {
                Location location1 = new Location();
                location1.setLocation(location);
                JSONObject address_components = result.getJSONObject("address_components");
                if (address_components != null) {
                    location1.setProvince(address_components.getString("province"));
                    location1.setCity(address_components.getString("city"));
                    location1.setDistrict(address_components.getString("district"));
                }
                JSONObject locations = result.getJSONObject("location");
                if (locations != null) {

                    location1.setLongitude(locations.getDouble("lng"));
                    location1.setLatitude(locations.getDouble("lat"));
                }
                return location1;
            }
        }
        return null;
    }

    private Location getFromAmap(String location, int x) {
        JSONObject jsonObject = null;
        try {
            jsonObject = getJson("https://restapi.amap.com/v3/geocode/geo?address=" +
                    URLEncoder.encode(location, "UTF-8")
                    + "&key="
                    + amapKey);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Integer status = jsonObject.getInteger("status");
        if (status == null) status = -1;
        if (status <= 0 && x < 2) {
            return getFromAmap(location, x + 1);
        } else if (x >= 2){
            if (status <= 0)
                System.err.println("获取失败：" + location+"\t"+jsonObject);
        }else {
            JSONArray geocodes = jsonObject.getJSONArray("geocodes");
            if (geocodes!=null&&geocodes.size()>0) {
                JSONObject jsonObject1 = geocodes.getJSONObject(0);
                Location location1 = new Location();
                location1.setLocation(location);
                location1.setProvince(jsonObject1.getString("province"));
                location1.setCity(jsonObject1.getString("city"));
                location1.setDistrict(jsonObject1.getString("district"));
                String[] locations = jsonObject1.getString("location").split(",");
                if (locations != null) {
                    location1.setLongitude(Double.parseDouble(locations[0]));
                    location1.setLatitude(Double.parseDouble(locations[1]));
                }
                return location1;
            }
        }
        return null;
    }

    private JSONObject getJson(String url){
        try {
            Connection.Response execute = Jsoup.connect(url)
                    .header("Accept", "*/*")
                    .header("Accept-Encoding", "gzip, deflate")
                    .header("Accept-Language","zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3")
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0")
                    .timeout(30000).ignoreContentType(true).execute();
            String body = execute.body();
            JSONObject jsonObject = JSONObject.parseObject(body);
            return jsonObject;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }
}
