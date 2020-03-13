package com.linghit.udf;

import com.linghit.util.TextUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 此UDF清洗手机机型，转为主要的几类手机，iphone、xiaomi、huawei之类的
 */
public class EquipmentUDF {

    private static Map<String, String> equipmentMap = new HashMap<>();
    private static Map<String, String> equipmentIdMap = new HashMap<>();

    static {
        System.out.println("测试:初始化equipment");
        InputStreamReader reader = new InputStreamReader(
                EquipmentUDF.class.getClassLoader().getResourceAsStream("equipment.properties"));
        Properties equipmentProp = new Properties();
        try {
            equipmentProp.load(reader);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        for (Object key : equipmentProp.keySet()) {
            equipmentMap.put(key.toString(), equipmentProp.getProperty(key.toString()));
        }

        // 说了你可能不信，直接读取第一行的中文会多出来空格，莫名其妙，换一种方式初始化数据
//        reader = new InputStreamReader(
//                EquipmentUDF.class.getClassLoader().getResourceAsStream("equipment_id.properties"));
//        equipmentProp = new Properties();
//        try {
//            equipmentProp.load(reader);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        for (Object key : equipmentProp.keySet()) {
//            equipmentIdMap.put(key.toString(), equipmentProp.getProperty(key.toString()));
//        }

        equipmentIdMap.put("华为", "HUAWEI");
        equipmentIdMap.put("三星", "SAMSUNG");
        equipmentIdMap.put("苹果", "iPhone");
        equipmentIdMap.put("魅族", "MEIZU");
        equipmentIdMap.put("小米", "XIAOMI");
        equipmentIdMap.put("中兴", "ZTE");
        equipmentIdMap.put("联想", "Lenovo");
        equipmentIdMap.put("索尼", "SONY");
        equipmentIdMap.put("摩托罗拉", "Motorola");
        equipmentIdMap.put("vivo", "vivo");
        equipmentIdMap.put("TCL", "TCL");
        equipmentIdMap.put("HTC", "HTC");
        equipmentIdMap.put("诺基亚", "NOKIA");
        equipmentIdMap.put("OPPO", "OPPO");
        equipmentIdMap.put("酷派", "Coolpad");
        equipmentIdMap.put("天语", "K-TOUCH");
        equipmentIdMap.put("金立", "GIONEE");

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * type : 1表示返回id，其余返回名称
     */
    public static String evaluate(String equipment, int type) {

        String equipment_id = "EquipmentUnknown";
        String equipment_name = "未知";

        if (equipment == null || "".equals(equipment)) {
            if (type == 1)
                return equipment_id;
            else
                return equipment_name;
        }

        for (String key : equipmentMap.keySet()) {

            if (equipment.toUpperCase().startsWith(key.toUpperCase())) {

                equipment_name = equipmentMap.get(key);
                equipment_id = equipmentIdMap.get(equipment_name);

                break;
            }
        }


//        if (type == 1)
//            return equipment_id;
//        else
//            return equipment_name;

        // 解析不出来就原样返回
        if (type == 1)
            return TextUtils.isEmpty(equipment_id) ? equipment : equipment_id;
        else
            return TextUtils.isEmpty(equipment_name) ? equipment : equipment_name;
    }

}
