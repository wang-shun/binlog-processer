package com.datatrees.datacenter.transfer.utility;

import java.util.UUID;

public class UuidUtil {
    public static String getUuidStr(){
        return UUID.randomUUID().toString();
    }
}
