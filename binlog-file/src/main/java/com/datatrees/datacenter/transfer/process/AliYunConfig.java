package com.datatrees.datacenter.transfer.process;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AliYunConfig {
    private static Logger LOG = LoggerFactory.getLogger(AliYunConfig.class);
    private static Properties properties = PropertiesUtility.defaultProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final DefaultProfile profile;
    private static final IAcsClient client;
    static {
        profile = DefaultProfile.getProfile(
                REGION_ID,
                ACCESS_KEY_ID,
                ACCESS_SECRET);
        client = new DefaultAcsClient(profile);
    }

    public static DefaultProfile getProfile() {
        return profile;
    }

    public static IAcsClient getClient() {
        return client;
    }
}
