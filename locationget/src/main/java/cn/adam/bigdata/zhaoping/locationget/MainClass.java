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
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;

public class MainClass {
    private static String[] tencentKey = {
            "VLABZ-TJNKF-326JS-NW6NB-H7Y6S-DJBVA",     //王恒兴
            "UXRBZ-UUI6X-FJI4B-ZJABP-KPZDS-6EBQD",  //缑鑫
            "UA4BZ-YB3WP-PSADF-VFOYF-N2HP5-JDBVY",   //喵喵
            "7YCBZ-N2BWX-WJV44-ZSZES-REMDK-4WB6B",
            "NOZBZ-A4K63-55A3S-3U5GB-LU7L7-VWFHI",
            "3WIBZ-6JKCX-WCW4Q-TBGOX-SRMOV-XVBVY",
            "INDBZ-CML6I-2RGGN-5CSBS-IG44V-WLBT6",
            "PE5BZ-HXY6P-TZZDM-VRBKC-ASDPJ-MHF26",
            "UHABZ-TBOWX-LJH4U-ZYUA3-6H5D6-MPBNJ"
    };


    private static String[] amapKeys = {
            "1e9534ddb6fc5c4ee9bf4d6f746ec71c",
            "f362bce4a14082653c56292ae23c1d89",
            "8f49272e49c67040245a35fab7a59155",
            "c9857e7adc149acc6d783d29d00acf05",
            "5aec35ad705af96d49970176e73317bf"
    };

    static ExecutorService executorService = Executors.newFixedThreadPool(3);
    public static void main(String[] args) {
        MainClass mainClass = new MainClass();

//        File f = new File(MainClass.class.getClassLoader().getResource("location.txt").getPath());
        File f = new File(MainClass.class.getClassLoader().getResource("companyname.txt").getPath());

        new Thread(new SaveTask()).start();
        int i = 1;
        try (
                Scanner sc = new Scanner(f);
        ) {
            boolean is = true;
            while (sc.hasNextLine()) {
                String s = sc.nextLine();

                executorService.execute(new JobTask(s));
            }
            executorService.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static volatile int i = 0;
    private static volatile int ii = 0;

    private synchronized static String getTencentKey() {
        if (i >= tencentKey.length) {
            i = 0;
        }
        String key = tencentKey[i];
        i++;
        return key;
    }
    private synchronized static String getAmapKey() {
        if (ii >= amapKeys.length) {
            ii = 0;
        }
        String key = amapKeys[ii];
        ii++;
        return key;
    }
    private static Location getFromTencent(String location, int x) {
        String key = getTencentKey();

        JSONObject jsonObject = null;
        try {
            jsonObject = getJson("https://apis.map.qq.com/ws/geocoder/v1/?address=" +
                    URLEncoder.encode(location, "UTF-8")
                    + "&key="
                    + key);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Integer status = jsonObject.getInteger("status");
        if (status == null) status = -1;
        if (status == -1 && x < 2) {
            return getFromTencent(location, x + 1);
        } else if (x >= 2 || (status >= 300 && status <= 400))
            return getFromAmap(location, 1);
        if (status != 0 && status != 101)
            System.err.println("Key失效:" + key);
        else if (status == 0) {
            JSONObject result = jsonObject.getJSONObject("result");
            if (result != null) {
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

    private static Location getFromAmap(String location, int x) {
        String key = getAmapKey();

        JSONObject jsonObject = null;
        try {
            jsonObject = getJson("https://restapi.amap.com/v3/geocode/geo?address=" +
                    URLEncoder.encode(location, "UTF-8")
                    + "&key="
                    + key);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Integer status = jsonObject.getInteger("status");
        if (status == null) status = -1;
        if (status <= 0 && x < 2) {
            return getFromAmap(location, x + 1);
        } else if (x >= 2) {
            if (status <= 0)
                System.err.println("获取失败：" + location + "\t" + jsonObject);
        } else {
            JSONArray geocodes = jsonObject.getJSONArray("geocodes");
            if (geocodes != null && geocodes.size() > 0) {
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

    private static JSONObject getJson(String url) {
        for (int j = 0; j < 3; j++) {

            try {
                FutureTask<JSONObject> result = new FutureTask<>(new Task(url));
                new Thread(result).start();
                return result.get();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
            }
        }
        return null;
    }

    private static JSONObject get(String url) {
        try {
            Connection.Response execute = Jsoup.connect(url)
                    .header("Accept", "*/*")
                    .header("Accept-Encoding", "gzip, deflate")
                    .header("Accept-Language", "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3")
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0")
                    .timeout(30000).ignoreContentType(true).execute();
            String body = execute.body();
            JSONObject jsonObject = JSONObject.parseObject(body);
            return jsonObject;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    private static class Task implements Callable<JSONObject> {

        String url;

        public Task(String url) {
            this.url = url;
        }

        @Override
        public JSONObject call() throws Exception {
            return get(url);
        }
    }

    private static class JobTask implements Runnable {

        String s;

        public JobTask(String s) {
            this.s = s;
        }

        @Override
        public void run() {
            try {
                Location fromTencent = new MainClass().getFromTencent(s, 1);

                SaveTask.putQueue(new Entry(s, fromTencent));
            }catch (Throwable e){
                e.printStackTrace();
            }
        }
    }
    static class Entry implements Map.Entry<String, Location>{
        private String key;
        private Location value;

        public Entry(String key, Location value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return this.key;
        }

        @Override
        public Location getValue() {
            return this.value;
        }

        @Override
        public Location setValue(Location value) {
            return this.value=value;
        }

    }
    private static class SaveTask implements Runnable {

        private static volatile LinkedBlockingQueue<Map.Entry<String, Location>> queue = new LinkedBlockingQueue<>();
        @Override
        public void run() {
            int i = 0;
                try (
                        PrintWriter p = new PrintWriter(new File(".\\l.txt"));
                        PrintWriter perr = new PrintWriter(new File(".\\lerr.txt"))
                ) {
                    while (true) {
                        if (executorService.isTerminated()){
                            System.out.println(new File(".").getPath());
                            break;
                        }

                        Map.Entry<String, Location> take = queue.take();
                        Location fromTencent = take.getValue();

                        if (fromTencent == null) {
                            perr.println(take.getKey());
                            perr.flush();
                            continue;
                        }
                        p.println(fromTencent.getProvince() + "\t"
                                + fromTencent.getCity() + "\t"
                                + fromTencent.getDistrict() + "\t"
                                + fromTencent.getLongitude() + "\t"
                                + fromTencent.getLatitude() + "\t"
                                + fromTencent.getLocation()
                        );
                            p.flush();

                        if (i % 20 == 0 || i == 0)
                            System.out.println(i+"%");
                        else
                            System.out.print(i+"%\t");

                        i += 1;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
        }

        public synchronized static void putQueue(Map.Entry<String, Location> l) {
            try {
                SaveTask.queue.put(l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}