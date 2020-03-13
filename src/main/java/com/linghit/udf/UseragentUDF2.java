package com.linghit.udf;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 减少耗时，一次性计算出各项结果
 */
public class UseragentUDF2 {

    private static UASparser uasParser = null;

    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
            // java.lang.UnsupportedClassVersionError:
            // cz/mallat/uasparser/UASparser : Unsupported major.minor version 51.0
            // 用jdk1.6测试时会报以上错，需要jdk1.7以上版本支持
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> evaluate(String useragent) {

        if (useragent == null || "".equals(useragent))
            return null;

        Map<String, String> resultMap = new HashMap<>();

        // 先进行初始化避免异常
        resultMap.put("sys", UserAgentInfo.UNKNOWN);
        resultMap.put("sys_version", UserAgentInfo.UNKNOWN);
        resultMap.put("browser", UserAgentInfo.UNKNOWN);
        resultMap.put("equipment_brand", UserAgentInfo.UNKNOWN);
        resultMap.put("equipment_brand_name", UserAgentInfo.UNKNOWN);
        resultMap.put("equipment_code", UserAgentInfo.UNKNOWN);

        try {
            UserAgentInfo userAgentInfo = UseragentUDF2.uasParser.parse(useragent);
            String sys = userAgentInfo.getOsFamily();
            String sys_version = userAgentInfo.getOsName();
            String browser = userAgentInfo.getUaFamily();
            String equipment = parseEquipmentCode(useragent);

            resultMap.put("sys", sys);
            resultMap.put("sys_version", sys_version);
            resultMap.put("browser", browser);
            resultMap.put("equipment_brand", equipment);
            resultMap.put("equipment_brand_name", equipment);
            resultMap.put("equipment_code", equipment);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }


    private static String parseEquipmentCode(String useragent) {
        String pTypeOs = parseUa(useragent);

        String ptNew;

        String ptype = pTypeOs.trim().split("build")[0];


        if (ptype.split(" ").length > 5) {
            ptNew = ptype.split(" ")[0];
        } else if (ptype.equals("")) {
            ptNew = "unkown";
        } else {
            ptNew = ptype.replace("/", "").trim();
        }

        return ptNew;

    }

    private static String parseUa(String useragent) {
        String ualower = useragent.toLowerCase().replace("u; ", "")
                .replace(" zh-cn;", "")
                .replace("; wv", "")
                .replace(")", ";")
                .replace("(", ";");
//	                          .replace("mozilla/5.0 (;","");

        String pt = "";

        if (ualower.startsWith("xiaomi")) {
            String[] xiaomiua = ualower.split(" ");
            if (xiaomiua.length < 3) {
//	                System.out.println(ualower);
            } else {
                pt = "xiaomi";
            }

            return pt;
        }

        if (ualower.startsWith("lovenote")) {
            String[] lua = ualower.replace(";;", ";").split(";");
            if (lua.length < 4) {
//	                System.out.println(ualower);
            } else {

                pt = lua[2];
            }
            return pt;
        }


        if (ualower.contains("windows")) {
            String[] wua = ualower.split(";");
            if (wua.length < 3) {
//	                System.out.println(ualower);
            } else {
                pt = "windows";
            }

            return pt;
        }


        if (ualower.startsWith("dayima")) {
            String[] dua = ualower.split(";");
            if (dua.length < 3) {
//	                System.out.println(ualower);
            } else {
                pt = dua[1];
            }

            return pt;
        }

        if (ualower.startsWith("lenovo")) {
            String[] leua = ualower.split(" ");
            if (leua.length < 3) {
//	                System.out.println(ualower);
            } else {
                pt = "lenovo";
            }

            return pt;
        }

        if (ualower.startsWith("huawei") || ualower.startsWith("honor")) {
            String[] heua = ualower.split(" ");
            if (heua.length < 3) {
//	                System.out.println(ualower);
            } else {
                pt = "huawei";
            }


            return pt;
        }

        if (ualower.startsWith("mozilla/") && !ualower.contains("windows")) {
            if (ualower.contains("android")) {
                String[] uasp = ualower.split(";");
                if (uasp.length < 4) {

                } else {
                    pt = uasp[3];
                }

                return pt;
            }

            if (ualower.contains("iphone") || ualower.contains("ipad") || ualower.contains("macintosh")) {
                String[] sp = ualower.split(";");
                if (sp.length < 3) {
//	                    System.out.println(ualower);
                } else {
                    pt = sp[1];
                }

                return pt;
            }

            if (ualower.contains("x11"))
                return "xiaomi";

            return pt;
        }

        return pt;
    }


}
