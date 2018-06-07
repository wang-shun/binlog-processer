package com.datatrees.datacenter.core.utility;

public class ReflectUtility {
    public static <T> T reflect(String cls) {
        try {
            Class<?> reflectCls = ClassLoader.getSystemClassLoader().loadClass(cls);
            return (T) reflectCls.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }
}
