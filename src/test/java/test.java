import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import com.alibaba.fastjson.JSON;

public class test {
    public static void main(String[] args){

        String json_file = "/Users/xufeng/userEvent.json";
        File file = new File(json_file);
        String jsonStr = JsonUtil.readJsonFile(file);
        Map jsonMap = (Map) JSON.parse(jsonStr);
        System.out.println(jsonMap);
        System.out.println(jsonMap.get("highlight"));

//        for (Object entry : jsonMap.entrySet()) {
//            System.out.println(entry.getKey()+"--"+entry.getValue());
//        }

        String jsonString = "{\"a\":{\"a1\":\"va1\",\"a2\":\"va2\"}}";
        JSONObject json= com.alibaba.fastjson.JSONObject.parseObject(jsonString);
        json.remove("a");
//        JSONObject aJson = json.getJSONObject("a");
//        aJson.remove("a1");

        System.out.println(json.toJSONString());

    }


}

/**
 * Json读取工具类
 */
class JsonUtil {
    private static final Logger logger = LogManager.getLogger(JsonUtil.class);
    /**
     * 读取json文件
     * @param jsonFile json文件名
     * @return 返回json字符串
     */
    public static String readJsonFile(File jsonFile) {
        String jsonStr = "";
        logger.info("————开始读取" + jsonFile.getPath() + "文件————");
        try {
            //File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            logger.info("————读取" + jsonFile.getPath() + "文件结束!————");
            return jsonStr;
        } catch (Exception e) {
            logger.info("————读取" + jsonFile.getPath() + "文件出现异常，读取失败!————");
            e.printStackTrace();
            return null;
        }
    }
}
