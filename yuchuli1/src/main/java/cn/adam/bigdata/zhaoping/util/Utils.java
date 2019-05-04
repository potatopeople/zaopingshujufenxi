package cn.adam.bigdata.zhaoping.util;

import cn.adam.bigdata.zhaoping.entity.Null;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class Utils {

    /**
     * 将CSVRecord转为List
     * @param record 传入CSVRecord
     * @return 返回ArrayList
     */
    public static List<String> csvRecordToList(CSVRecord record){
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

            if (isCollection){
                return list;
            } else{
                StringBuilder sb = new StringBuilder(list.get(0).toString());
                for (int i = 1; i < list.size(); i++){
                    sb.append(s).append(list.get(i));
                }
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
        Method method = o.getClass().getMethod(m);
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
    public static Integer equalsClassAFromB(Class a, Class b){
        return equalsClass(a, b, 0, 1);
    }

    /**
     * 判断a是否是b的父级，如果返回值为null，没有继承关系，如果等于0表示两个类一样，
     * 如果大于0，a是b的父级，值得大小为多少级，
     * @param a 传入class对象
     * @param b 传入class对象
     * @return 返回结果
     */
    public static Integer equalsClassBFromA(Class a, Class b){
        return equalsClass(b, a, 0, 1);
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

        Class[] ccs = c.getInterfaces();
        for (Class cc : ccs) {
            re = equalsClass(cc, b, index+step, step);
            if (re != null)
                break;
        }

        return re;
    }

}
