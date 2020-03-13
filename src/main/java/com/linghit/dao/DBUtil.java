package com.linghit.dao;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

public class DBUtil {
	public static ComboPooledDataSource mysql_pool = null;
	public static ComboPooledDataSource presto_pool = null;
	public static ComboPooledDataSource hive_pool = null;
	public static ComboPooledDataSource impala_pool = null;
	
	public static ComboPooledDataSource initOrGetPool(String db){
		
		if(db.equals("mysql1")){
			if(mysql_pool == null){
				mysql_pool = new ComboPooledDataSource("mysql1");
			 }
			return mysql_pool;
		}
		else if(db.equals("mysql2")){
			if(mysql_pool == null){
				mysql_pool = new ComboPooledDataSource("mysql2");
			}
			return mysql_pool;
		}
		else if(db.equals("rds")){
			if(mysql_pool == null){
				mysql_pool = new ComboPooledDataSource("rds");
			 }
			return mysql_pool;
		}
		else if(db.equals("test")){
			if(mysql_pool == null){
				mysql_pool = new ComboPooledDataSource("test");
			}
			return mysql_pool;
		}
		else if(db.equals("mysql4")){
			if(mysql_pool == null){
				mysql_pool = new ComboPooledDataSource("mysql4");
			 }
			return mysql_pool;
		}
		else if(db.equals("presto")){
			if(presto_pool == null){
				presto_pool = new ComboPooledDataSource("presto");
			 }
			return presto_pool;
		}
		else if(db.equals("hive")){
			if(hive_pool == null){
				hive_pool = new ComboPooledDataSource("hive");
			 }
			return hive_pool;
		}
		else if(db.equals("hive_emr")){
			if(hive_pool == null){
				hive_pool = new ComboPooledDataSource("hive_emr");
			 }
			return hive_pool;
		}
		else if(db.equals("impala")){
			if(impala_pool == null){
				impala_pool = new ComboPooledDataSource("impala");
			 }
			return impala_pool;
		}
		else
			return null;
		
	}
	
	public static void close(){
		
		if(mysql_pool != null)
			mysql_pool.close();
		
		if(presto_pool != null)
			presto_pool.close();
		
		if(hive_pool != null)
			hive_pool.close();
		
		if(impala_pool != null)
			impala_pool.close();
		
	}
	
	public static void refreshPool(String db){
		
		if(db.equals("mysql1")){
			if(mysql_pool != null)
				mysql_pool.close();
			
			mysql_pool = new ComboPooledDataSource("mysql1");
		}
		else if(db.equals("rds")){
			
			if(mysql_pool != null)
				mysql_pool.close();
			
			mysql_pool = new ComboPooledDataSource("rds");
		}
		
		else if(db.equals("test")){

			if(mysql_pool != null)
				mysql_pool.close();

			mysql_pool = new ComboPooledDataSource("test");
		}
		else if(db.equals("mysql4")){
			
			if(mysql_pool != null)
				mysql_pool.close();
			
			mysql_pool = new ComboPooledDataSource("mysql4");
		}
		else if(db.equals("presto")){
			
			if(presto_pool != null)
				presto_pool.close();
			
			presto_pool = new ComboPooledDataSource("presto");
		}
		else if(db.equals("hive")){
			if(hive_pool != null)
				hive_pool.close();

			hive_pool = new ComboPooledDataSource("hive");
		}
		else if(db.equals("impala")){
			
			if(impala_pool != null)
				impala_pool.close();
				
			impala_pool = new ComboPooledDataSource("impala");
		}
		
	}
	
	
	public static Connection getConnection(String db){
		
		try {
			return initOrGetPool(db).getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	
	public static Object queryOne(String db,String sql){
		Connection conn = null;
		Statement stmt = null;
		
		Object object = null;
		
		try {
			
			System.out.println("-------------------------------------");

			conn = getConnection(db);
			
			stmt = conn.createStatement();
			
			long start = System.currentTimeMillis();

			System.out.println("执行语句：" + sql);

			//查询最大耗时不超过6分钟
			//stmt.setQueryTimeout(360);
			
			ResultSet res = stmt.executeQuery(sql);
			
			//int column_count = res.getMetaData().getColumnCount();
			
			while (res.next()) {
				object = res.getLong(1);
				System.out.println(object);
			}
			
			long end = System.currentTimeMillis();

			System.out.println("耗时(ms):" + (end - start));
			System.out.println("-------------------------------------");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (stmt != null) {
					stmt.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		return object;
	}
	

	public static Vector<String[]> query(String db,String sql){
		Connection conn = null;
		Statement stmt = null;

		Vector<String[]> result = new Vector<String[]>();
		
		try {
			

			System.out.println("-------------------------------------");

			conn = getConnection(db);
			
			stmt = conn.createStatement();
			
			long start = System.currentTimeMillis();

			System.out.println("执行语句：" + sql);

			//查询最大耗时不超过6分钟
			//stmt.setQueryTimeout(360);
			
			
			ResultSet res = stmt.executeQuery(sql);
			
			int column_count = res.getMetaData().getColumnCount();
			
			while(res.next()){
				
				String[] arr = new String[column_count];
				
				for(int i=0;i<column_count;i++){
					arr[i] = res.getString(i+1);
				}
				
				result.add(arr);
			}
			
			long end = System.currentTimeMillis();

			System.out.println("耗时(ms):" + (end - start));
			System.out.println("-------------------------------------");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (stmt != null) {
					stmt.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		return result;
	}


	public static int executeUpdate(String db,String sql){
		Connection conn = null;
		Statement stmt = null;

		try {

			System.out.println("-------------------------------------");
			
			conn = getConnection(db);
			
			stmt = conn.createStatement();
			
			long start = System.currentTimeMillis();

			System.out.println("执行语句：" + sql);

			//查询最大耗时不超过6分钟
			//stmt.setQueryTimeout(50);
			
			int result = stmt.executeUpdate(sql);

			long end = System.currentTimeMillis();

			System.out.println("耗时(ms):" + (end - start));
			System.out.println("-------------------------------------");
			
			return result;
			
		} catch (Exception e) {
			
			e.printStackTrace();
			return -1;
			
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (stmt != null) {
					stmt.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}




	public static String queryAppNameById(String db,String appId){
		Connection conn = null;
		Statement stmt = null;

		String object = null;
        String sql = "select app_name from dp_public.app_info where app_id = '"+appId+"' limit 1";
		try {
			conn = getConnection(db);
			stmt = conn.createStatement();
			System.out.println("执行语句：" + sql);
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				object = res.getString(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return object;
	}




}
