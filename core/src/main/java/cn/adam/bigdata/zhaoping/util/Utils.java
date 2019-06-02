package cn.adam.bigdata.zhaoping.util;

import cn.adam.bigdata.zhaoping.entity.Null;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

@Slf4j
public class Utils {

    public static <T> T copyFieldToObject(Object from, T to) {
        Class fclazz = from.getClass();
        Class tclazz = to.getClass();

        Field[] fromField = fclazz.getDeclaredFields();
        for (Field f : fromField){
            f.setAccessible(true);
            try {
                Object o = f.get(from);
                if (o != null){
                    Field toField = tclazz.getDeclaredField(f.getName());
                    toField.setAccessible(true);
                    toField.set(to, o);
                }
            } catch (Exception e) {
                log.error("操作对象指出错！", e);
            }
        }
        return to;
    }
    public static Object emptyFieldToNull(Object o) {
        Class clazz = o.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for(Field f : declaredFields){
            f.setAccessible(true);
            try {
                Object re = f.get(o);
                if (re != null&&"".equals(re.toString())){
                    f.set(o, null);
                }
            } catch (IllegalAccessException e) {
                log.error("获取属性值出错！", e);
            }
        }

        return o;
    }
    public static CSVRecord csvstrToCSVRecord(String csvstr, CSVFormat format) {
        CSVRecord record = null;
        try {
            CSVParser parse = CSVParser.parse(csvstr, format);
            Iterator<CSVRecord> iterator = parse.iterator();
            if (iterator.hasNext())
                record = iterator.next();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    public static String objectToCsvstr(Object o, CSVFormat format) {
        StringBuffer sb = new StringBuffer();

        Iterable i = null;
        if (equalsClassAFromB(o.getClass(), Iterable.class) > 0){
            i = (Iterable) o;
        }else {
            String[] sss = format.getHeader();
            if (sss != null && sss.length == 0)
                sss = null;
            i = objectToList(o, sss);
        }

        try {
            CSVPrinter printer = new CSVPrinter(sb, format);

            printer.printRecord(i);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static List<Object> objectToList(Object o, String[] ss){
        List list = null;
        Object[] os = new Object[ss.length];;
        if (ss == null)
            list = new ArrayList<>();

        if (equalsClassAFromB(o.getClass(), Map.class) > 0){
            Map<Object, Object> m = (Map) o;
            if (ss == null)
                list.addAll(m.values());
            else {
                Collection<Object> set = m.values();
//                list = new ArrayList(set.size());
//                list = new ArrayList(ss.length);
                for (Map.Entry e : m.entrySet()) {
                    int index = 0;
                    if ((index = Arrays.binarySearch(ss, e.getKey())) >= 0) {
//                        list.set(index, e.getValue());
                        os[index] = e.getValue();
                    }
                }
            }
        }else {
            Class clazz = o.getClass();
            Field[] declaredFields = clazz.getDeclaredFields();
            if (ss == null){
//                list = new ArrayList();
                for (Field f : declaredFields) {
                    f.setAccessible(true);
                    try {
                        list.add(f.get(o));
                    } catch (IllegalAccessException e) {log.error("出错！", e);}
                }
            }else {

//                list = new ArrayList(declaredFields.length);
                for (Field f : declaredFields) {
                    f.setAccessible(true);
                    int index = 0;
                    if ((index = Arrays.binarySearch(ss, f.getName())) >= 0) {
                        try {
//                            list.set(index, f.get(o));
                            os[index] = f.get(o);
                        } catch (IllegalAccessException e) {
                            log.error("出错！", e);
                        }
                    }
                }
            }
        }

        if (ss != null)
            list = Arrays.asList(os);
        return list;
    }

    public static Object mapToObject(Map<String, ?> map, Object o) {
        Class clazz = o.getClass();
        try {
            for (Map.Entry<String, ?> e : map.entrySet()) {
                Field declaredField = clazz.getDeclaredField(e.getKey());
                declaredField.setAccessible(true);
                declaredField.set(o, e.getValue());
            }
        } catch (Exception e) {
            log.error("实例赋值出错！", e);
        }
        return o;
    }

    public static void writeDataOutput(DataOutput dataOutput, Object o) {
        Class clazz = o.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field f: declaredFields) {
            f.setAccessible(true);
            try {
                Object oo = f.get(o);
                dataOutput.writeUTF(oo == null?"":oo.toString());
            } catch (Exception e) {
                log.error("序列化写出出错！",e);
            }
        }
    }
    public static void readDataInput(DataInput dataInput, Object o) {
        Class clazz = o.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field f: declaredFields) {
            f.setAccessible(true);
            try {
                f.set(o, dataInput.readUTF());
            }  catch (Exception e) {
                log.error("序列化读取出错！",e);
            }
        }
    }

    /**
     * 将CSVRecord转为List
     * @param record 传入CSVRecord
     * @return 返回ArrayList
     */
    public static List<String> csvstrRecordToList(CSVRecord record){
        Iterator<String> iterator = record.iterator();
        List<String> list = new ArrayList<>();
        while (iterator.hasNext()){
            list.add(iterator.next());
        }
        return list;
    }

    /**
     * 传入一个集合或者数组，执行指定的方法，将返回值按给定的类型打包返回,
     * @param o 传入集合或者数组
     * @param mn 执行指定的方法名字
     * @param s 如果是返回String，可制定间隔符
     * @param isCollection 制定返回List还是字符串，true为List
     * @return 返回结果，字符串，或者ArrayList
     */
    public static <T> Object getAll(Object o, String mn, String s, boolean isCollection) {
        log.debug(o.getClass().getName());
        log.debug(isCollection + "");

        List<Object> list = new ArrayList<>();
        try {
            if (o.getClass().getName().startsWith("[L")) {
                for (Object tmp : (Object[]) o)
                    list.add(invoke(tmp, mn));
            }else if (equalsClassAFromB(o.getClass(), Iterable.class) >= 0){
                for (Object tmp : (Iterable<?>) o)
                    list.add(invoke(tmp, mn));
            }else if (equalsClassAFromB(o.getClass(), Iterator.class) >= 0) {
                Iterator i = (Iterator) o;
                while (i.hasNext())
                    list.add(invoke(i.next(), mn));
            }

            if (list.size() <= 0)
                return null;

            if (isCollection){
                return list;
            } else{
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < list.size(); i++){
                    sb.append(list.get(i)).append(s);
                }
                sb.deleteCharAt(sb.length()-1);
                return sb.toString();
            }

        } catch (Exception e) {
            log.error("执行指定方法出错!", e);
        }
        return Null.getInstance();
    }

    /**
     * 执行对象的指定方法
     * @param o 传入对象
     * @param m 传入方法名
     * @return 返回结果
     */
    public static Object invoke(Object o, String m) throws Exception {
        return invoke(o, m, new Class[0], new Object[0]);
    }
    public static Object invoke(Object o, String m, Class c, Object os) throws Exception {
        return invoke(o, m, new Class[]{c}, new Object[]{os});
    }
    public static Object invoke(Object o, String m, Class[] cs, Object[] os) throws Exception {
        Method method = o.getClass().getMethod(m, cs);
        return method.invoke(o, os);
    }

    /**
     * 判断两个类的关系，如果返回值为null，没有继承关系，如果等于0表示两个类一样，
     * 如果大于0，b是a的父级，值得大小为多少级，如果小于0，反之
     * @param a 传入class对象
     * @param b 传入class对象
     * @return 返回结果
     */
    public static Integer equalsClass(Class a, Class b){
        Integer re = equalsClass(a, b, 0, 1);
        if (re != null)
            return re;
        re = equalsClass(b, a, 0, -1);
        return re;
    }

    /**
     * 判断b是否是a的父级，如果返回值为null，没有继承关系，如果等于0表示两个类一样，
     * 如果大于0，b是a的父级，值得大小为多少级，
     * @param a 传入class对象
     * @param b 传入class对象
     * @return 返回结果
     */
    public static int equalsClassAFromB(Class a, Class b){
        Integer i = equalsClass(a, b, 0, 1);
        return i == null ? -1 : i;
    }

    /**
     * 判断a是否是b的父级，如果返回值为null，没有继承关系，如果等于0表示两个类一样，
     * 如果大于0，a是b的父级，值得大小为多少级，
     * @param a 传入class对象
     * @param b 传入class对象
     * @return 返回结果
     */
    public static int equalsClassBFromA(Class a, Class b){
        Integer i = equalsClass(b, a, 0, 1);
        return i == null ? -1 : i;
    }

    private static Integer equalsClass(Class a, Class b, int index, int step){
        if (a == null)
            return null;
        if (a.equals(b))
            return index;

        Integer re;
        Class c = a.getSuperclass();
        re = equalsClass(c, b, index+step, step);
        if (re != null)
            return re;

        Class[] ccs = a.getInterfaces();
        for (Class cc : ccs) {
            re = equalsClass(cc, b, index+step, step);
            if (re != null)
                break;
        }

        return re;
    }

}
