package com.datatrees.datacenter.transfer.utility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpMatchUtility {
    public static boolean isboolIp(String ipAddress) {

        if (ipAddress.length() < 7 || ipAddress.length() > 15) {
            return false;
        }
/**
 * 判断IP格式和范围
 */
        String rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pat = Pattern.compile(rexp);
        Matcher mat = pat.matcher(ipAddress);
        boolean flag = mat.find();
        return flag;
    }
}
