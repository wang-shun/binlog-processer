package com.datatrees.datacenter.core.utility;

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
        String rexp2 = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(_(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";

        Pattern pat = Pattern.compile(rexp);
        Pattern pat2 = Pattern.compile(rexp2);
        Matcher mat = pat.matcher(ipAddress);
        Matcher mat2=pat2.matcher(ipAddress);
        boolean flag = mat.find()||mat2.find();
        return flag;
    }
}
