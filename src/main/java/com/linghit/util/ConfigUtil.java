package com.linghit.util;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigUtil {

    public static Map<String, String> configMap = new HashMap<>();

    public static void init() throws Exception {

        System.out.println("config init..");
        InputStreamReader reader = new InputStreamReader(ConfigUtil.class.getClassLoader().getResourceAsStream("application.properties"));
        Properties prop = new Properties();
        prop.load(reader);

        for (Object key : prop.keySet()) {
            System.out.println(key.toString() + " : " + prop.getProperty(key.toString()));
            configMap.put(key.toString(), prop.getProperty(key.toString()));
        }

        reader.close();
        System.out.println("config init success ..");

    }

    public static String getProperties(String key) {
        return configMap.get(key);
    }


}
