package com.linghit.udf;

public class PhoneUDF {

	public static String evaluate(String phone) {
		
		String result = "";
		
		if(phone == null || phone.trim().equals(""))
			return result ;
		
		result = phone.trim();
		
		if(result.contains("+86")){
			result = result.replace("+86", "");
		}
		
		return result.trim();
		
	}

}
