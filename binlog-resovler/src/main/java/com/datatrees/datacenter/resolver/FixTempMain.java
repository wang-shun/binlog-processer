package com.datatrees.datacenter.resolver;

import com.datatrees.datacenter.core.utility.PropertiesUtility;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FixTempMain {

  static void callMain(Method mainMethod, String[] args)
    throws InvocationTargetException, IllegalAccessException {
    switch (args.length) {
      case 0:
        mainMethod.invoke(null, new Object[]{null});
        break;
      case 1:
        mainMethod.invoke(null, new Object[]{new String[]{args[0]}});
        break;
      case 2:
        mainMethod.invoke(null, new Object[]{new String[]{args[0], args[1]}});
        break;
      case 3:
        mainMethod.invoke(null, new Object[]{new String[]{args[0], args[1], args[2]}});
        break;
    }
  }

  public static void main(String[] args) {
    java.util.Properties value = PropertiesUtility.defaultProperties();
    if (value.getProperty("SERVER_TYPE").equalsIgnoreCase("aliyun")) {
      FixTempFileTask.main(args);
    } else {
      FixTempFileTaskV2.main(args);
    }
//    java.util.Properties value = PropertiesUtility.defaultProperties();
//    try {
//      Class mainClass =
//        value.getProperty("SERVER_TYPE").equalsIgnoreCase("aliyun") ? Class
//          .forName("com.datatrees.datacenter.resolver.FixTempFileTask")
//          : Class.forName("com.datatrees.datacenter.resolver.FixTempFileTaskV2");
//      Method mainMethod = null;
//      if (null != mainClass) {
//        try {
//          mainMethod = mainClass.getMethod("main", String[].class);
//          callMain(mainMethod, args);
//        } catch (NoSuchMethodException e) {
//          return;
//        } catch (IllegalAccessException e) {
//          e.printStackTrace();
//        } catch (InvocationTargetException e) {
//          e.printStackTrace();
//        } finally {
//        }
//      }
//    } catch (ClassNotFoundException e) {
//      return;
//    }
  }
}
