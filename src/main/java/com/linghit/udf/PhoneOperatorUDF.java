package com.linghit.udf;

/*
 * 清洗手机运营商
 */
public class PhoneOperatorUDF {

	
	/*
	 * type : 1表示返回id，其余返回名称
	 */
	public static String evaluate(String phoneOperator,int type){
		
		String phoneOperator_id = "PhoneOperatorUnknown";
		String phoneOperator_name = "未知";
		
		if(phoneOperator == null || "".equals(phoneOperator)){
			if(type == 1)
				return phoneOperator_id;
			else
				return phoneOperator_name;
		}
		
		if(phoneOperator.contains("中國電信") || phoneOperator.contains("中国电信") || phoneOperator.contains("China Telecom")){
			
			phoneOperator_id = "ChinaTelecom";
			phoneOperator_name = "中国电信";
			
		}else if(phoneOperator.contains("中國移動") || phoneOperator.contains("中国移动") || phoneOperator.contains("CMCC") || phoneOperator.contains("China Mobile")){
			
			phoneOperator_id = "ChinaMobile";
			phoneOperator_name = "中国移动";
			
		}else if(phoneOperator.contains("中國聯通") || phoneOperator.contains("中国联通") || phoneOperator.contains("联通新时空") || phoneOperator.contains("China Unicom") ){
			
			phoneOperator_id = "ChinaUnicom";
			phoneOperator_name = "中国联通";
			
		}
		
		
		if(type == 1)
			return phoneOperator_id;
		else
			return phoneOperator_name;
	}
	
}
