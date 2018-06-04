package com.datatrees.datacenter.transfer.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author personalc
 */
public class TimeUtil {
    /**
     * 将特定格式的日期时间转化为时间戳
     *
     * @param timeStr yyyy-MM-dd'T'HH:mm:ss'Z'
     * @return 对应的时间戳
     */
    public static long utc2TimeStamp(String timeStr) {
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss'Z'");
        long timeStamp = 0;
        try {
            Date date = formatter.parse(timeStr);
            timeStamp = date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timeStamp;
    }

    /**
     * 特殊时间类型向普通时间类型转化
     *
     * @param timeStr
     */
    public static String utc2Common(String timeStr) {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = null;
        try {
            Date date = sdf1.parse(timeStr);
            str = sdf2.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return str;
    }

    /**
     * 不同日期格式之间转换
     *
     * @param formatOld 旧日期格式
     * @param formatNew 新日期格式
     * @param timeStr   旧日期时间
     * @return 新日期时间
     */
    public static String dateToDate(String formatOld, String formatNew, String timeStr) {
        SimpleDateFormat sdf1 = new SimpleDateFormat(formatOld);
        SimpleDateFormat sdf2 = new SimpleDateFormat(formatNew);
        String str = null;
        try {
            Date date = sdf1.parse(timeStr);
            str = sdf2.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(str);
        return str;
    }

    public static String timeStamp2DateStr(long timeStamp, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String dateStr = null;
        try {
            Date date = sdf.parse(sdf.format(timeStamp));
            dateStr = sdf.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(dateStr);
        return dateStr;
    }

    /**
     * 将java.sql.Timestamp对象转化为String字符串
     *
     * @param time      要格式的java.sql.Timestamp对象
     * @param strFormat 输出的String字符串格式的限定（如："yyyy-MM-dd HH:mm:ss"）
     * @return 表示日期的字符串
     */
    public static String dateToStr(java.sql.Timestamp time, String strFormat) {
        DateFormat df = new SimpleDateFormat(strFormat);
        String str = df.format(time);
        return str;
    }

    /**
     *
     * @param strDate
     * @param dateFormat
     * @return
     */
    public static Date strToUtilDate(String strDate, String dateFormat) {
        SimpleDateFormat sf = new SimpleDateFormat(dateFormat);
        Date date = null;
        try {
            date = sf.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }


    public static void main(String[] args) {
        //timeStamp2Date(1527165377);
        dateToDate("yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd HH:mm:ss", "2018-10-11T11:22:33Z");
        timeStamp2DateStr(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ss'Z'");
    }
}
