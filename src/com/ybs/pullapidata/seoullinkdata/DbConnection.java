package com.ybs.pullapidata.seoullinkdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DbConnection 
{
	private Statement stmt;
    private ResultSet result;
    private Connection conn;
    private String DbHost;
	private String DbName;
    private String DbUser;
    private String DbPass;
    DbConnection(String dbhost, String dbname, String dbuser, String dbpass)
    {
    	DbHost = dbhost;
    	DbName = dbname;
    	DbUser = dbuser;
    	DbPass = dbpass;
    }
    public String getDbHost() {
		return DbHost;
	}
	public void setDbHost(String dbHost) {
		DbHost = dbHost;
	}
	public String getDbName() {
		return DbName;
	}
	public void setDbName(String dbName) {
		DbName = dbName;
	}
	public String getDbUser() {
		return DbUser;
	}
	public void setDbUser(String dbUser) {
		DbUser = dbUser;
	}
	public String getDbPass() {
		return DbPass;
	}
	public void setDbPass(String dbPass) {
		DbPass = dbPass;
	}
	public ResultSet getResult() {
		return result;
	}
    
    public int Connect()
    {
    	try 
    	{
			Class.forName("com.mysql.jdbc.Driver");
			String dbInfo = "jdbc:mysql://" + DbHost + "/" + DbName;
			conn = DriverManager.getConnection(dbInfo, DbUser, DbPass);
			stmt = conn.createStatement();
		} catch (SQLException e) 
    	{
			e.printStackTrace();
			return -1;
		} catch (ClassNotFoundException e1) 
    	{
			e1.printStackTrace();
			return -2;
		}
    	return 0;
    }
    
    public int runQuery(String query)
    {
    	try {
			result = stmt.executeQuery(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
    	return 0;
    }
    
    public int update(String query)
    {
    	try {
			if(stmt.execute(query) == true)
			{
				return 1;
			}
			return 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
    }
}
