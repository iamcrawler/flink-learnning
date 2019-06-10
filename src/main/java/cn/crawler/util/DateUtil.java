package cn.crawler.util;

import java.util.Date;

/**
 * Created by liuliang on 2019/5/18.
 */
public class DateUtil {

    public static String format(Date date){
        java.text.DateFormat format1 = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return format1.format(date);
    }

}
