package com.linghit.udf;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;
import java.util.Map;

public class UseragentUDF {

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

    public static String evaluate(String useragent, String type) {

        if (useragent == null || "".equals(useragent))
            return null;

        String result = null;

        try {
            UserAgentInfo userAgentInfo = UseragentUDF.uasParser.parse(useragent);

            switch (type) {
                case "sys":
                    result = userAgentInfo.getOsFamily();
                    break;
                case "sys_version":
                    result = userAgentInfo.getOsName();
                    break;
                case "browser":
                    result = userAgentInfo.getUaFamily();
                    break;
                case "equipment_brand":
                    result = parseEquipmentCode(useragent);
                    break;
                case "equipment_brand_name":
                    result = parseEquipmentCode(useragent);
                    break;
                case "equipment_code":
                    result = parseEquipmentCode(useragent);
                    break;
                default:
                    result = "";
                    break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }


    private static String parseEquipmentCode(String useragent) {
        String pTypeOs = parseUa(useragent);

        String ptNew = "";

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


    public static void main(String[] args) {

        //String useragent="Mozilla/5.0 (Linux; Android 8.0.0; LDN-AL00 Build/HUAWEILDN-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/68.0.3440.91 Mobile Safari/537.36 ssy={ECalendar;V7.3.5;huawei;101110501;WIFI;libertyad;ebrowser;suid=;device_id=c8c1135ce4dd43f5c8e2c724e9f49068;}";

        long start = System.currentTimeMillis();

        String useragent = "Mozilla/5.0 (Linux; Android 8.0.0; BLN-AL40 Build/HONORBLN-AL40; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/69.0.3497.100 Mobile Safari/537.36 ssy={ECalendar;V7.3.5;huawei;101180713;WIFI;libertyad;ebrowser;suid=;device_id=0eb10a3c2f6e4a6808a6f4c0d125c30a;}";

        String equipment_brand = UseragentUDF.evaluate(useragent, "equipment_brand");
        String equipment_brand_name = UseragentUDF.evaluate(useragent, "equipment_brand_name");
        String equipment_code = UseragentUDF.evaluate(useragent, "equipment_code");
        String sys = UseragentUDF.evaluate(useragent, "sys");
        String sys_version = UseragentUDF.evaluate(useragent, "sys_version");
        String browser = UseragentUDF.evaluate(useragent, "browser");

        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        Map<String, String> map = UseragentUDF2.evaluate(useragent);

        System.out.println(System.currentTimeMillis() - start);
        System.out.println(map);


    }

}
