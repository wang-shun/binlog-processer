package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.datatrees.datacenter.core.utility.PropertiesUtility;

import java.util.Properties;

public class AliYunConfig {
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final DefaultProfile PROFILE;
    private static final IAcsClient CLIENT;
    static {
        PROFILE = DefaultProfile.getProfile(
                REGION_ID,
                ACCESS_KEY_ID,
                ACCESS_SECRET);
        CLIENT = new DefaultAcsClient(PROFILE);
    }

    public static DefaultProfile getProfile() {
        return PROFILE;
    }

    public static IAcsClient getClient() {
        return CLIENT;
    }
}
