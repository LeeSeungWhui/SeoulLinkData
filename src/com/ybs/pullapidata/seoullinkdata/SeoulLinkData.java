package com.ybs.pullapidata.seoullinkdata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.ybs.pullapidata.seoullinkdata.ApiConnection;
import com.ybs.pullapidata.seoullinkdata.DbConnection;

public class SeoulLinkData 
{
	static public ApiConnection apiconnection;
	static public String BaseDate, BaseTime;
	public static void main(String args[]) throws IOException, SQLException
	{
		// DB 연결
		String host = "192.168.0.53";
		String name = "HVI_DB";
		String user = "root";
		String pass = "dlatl#001";
		DbConnection dbconnection = new DbConnection(host, name, user, pass);
	    dbconnection.Connect();
	    
	    // 기존 DB 테이블 비우기
	    dbconnection.update("delete from SEOUL_LINK");
	    
	   // 서울시 도로축 정보 연계
	    apiconnection = new ApiConnection();
	    String[] RoadDiv = {"02","03", "04", "05"};
	    List<String> Axis_cd = null;
	    List<String> Axis_name = null;
	    for(String div : RoadDiv)
	    {
	    	apiconnection.setUrl("http://openapi.seoul.go.kr:8088/7a7466686f73756e3937547067797a/xml/RoadInfo/1/909/" + div);
	    	apiconnection.pullData();
	    	Axis_cd = apiconnection.getResult("axis_cd");
	    	Axis_name = apiconnection.getResult("axis_name");
	    }
	    
	    // 서울시 링크정보 연계
	    String FileName = "SEOUL_LINK.csv";
	    List<String> LinkColumn = new ArrayList<String>();
	    LinkColumn.add("axis_cd");
	    LinkColumn.add("axis_dir");
	    LinkColumn.add("link_seq");
	    LinkColumn.add("link_id");
	    LinkColumn.add("axis_name");
	    BufferedWriter bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter, LinkColumn);
	    for(int i = 0; i < Axis_cd.size(); i++)
	    {
	    	apiconnection.setUrl("http://openapi.seoul.go.kr:8088/7a7466686f73756e3937547067797a/xml/LinkWithLoad/1/999/" + Axis_cd.get(i));
	    	apiconnection.pullData();
	    	List<List<String>> LinkData = new ArrayList<List<String>>(); // csv파일 쓰기위한 변수
	    	for(int j = 0; j < LinkColumn.size() -1; j++)
	    	{
	    		LinkData.add(apiconnection.getResult(LinkColumn.get(j)));
	    	}
	    	List<String> Axis_name_tmp = new ArrayList<String>(); // csv생성을 위한 임시 변수
	    	for(int j = 0; j < LinkData.get(0).size(); j++)
	    	{
	    		Axis_name_tmp.add(Axis_name.get(i));
	    	}
	    	LinkData.add(Axis_name_tmp);
	    	WriteCSV(bufWriter, LinkData);
	    }
		bufWriter.close();
		
	    // DB에 입력
	    String sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE SEOUL_LINK FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES(";
	    int i = 0;
	    for(; i < LinkColumn.size() - 1; i++)
	    {
	    	sql += LinkColumn.get(i) + ",";
	    }
	    sql += LinkColumn.get(i) + ")";
	    dbconnection.update(sql);
	    
	    // 서울시 링크 vertex 연계
	    // DB에서 LinkID 가져옴
	    List<String> Link_id = new ArrayList<String>(); // 링크 id
	    sql = "select LINK_ID from SEOUL_LINK";
	    dbconnection.runQuery(sql);
	   while(dbconnection.getResult().next())
	   {
		   Link_id.add(dbconnection.getResult().getString("LINK_ID"));
	   }
	   
	   // API에서 vertex정보 가져와서 csv파일로 저장	    
	   List<String> VertexColumn = new ArrayList<String>();
	   VertexColumn.add("link_id");
	   VertexColumn.add("ver_seq");
	   VertexColumn.add("grs80tm_x");
	   VertexColumn.add("grs80tm_y");
	   FileName = "SEOUL_LINK_VERTEX.csv";
	   bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter, VertexColumn);
	   for(String link_id : Link_id)
	   {
		   apiconnection.setUrl("http://openapi.seoul.go.kr:8088/7a7466686f73756e3937547067797a/xml/LinkVerInfo/1/999/" + link_id);
		   apiconnection.pullData();
		   List<List<String>> VertexData = new ArrayList<List<String>>(); // csv파일 쓰기위한 변수
	    	for(String s : VertexColumn)
	    	{
	    		VertexData.add(apiconnection.getResult(s));
	    	}
	    	WriteCSV(bufWriter, VertexData);
	   }
	   bufWriter.close();
	   
	   // DB입력
	   sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE SEOUL_LINK_VERTEX FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES(";
	    i = 0;
	    for(; i < VertexColumn.size() - 1; i++)
	    {
	    	sql += VertexColumn.get(i) + ",";
	    }
	    sql += VertexColumn.get(i) + ")";
	    dbconnection.update(sql);
	}
	
	public static void CreateCSV(BufferedWriter bufWriter, List<String> Column)
	{
		try
		{
			int i = 0;
			for(; i < Column.size() - 1; i++)
			{
				bufWriter.write("\"" + Column.get(i) + "\",");
			}
			bufWriter.write("\"" + Column.get(i) + "\"");
			bufWriter.newLine();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void WriteCSV(BufferedWriter bufWriter, List<List<String>> datalist) throws IOException
	{
//		System.out.println(datalist.get(0).size() + " " + datalist.get(1).size()+ " " + datalist.get(2).size()+ " " + datalist.get(3).size()+ " " + datalist.get(4).size()+ " " + datalist.get(5).size()+ " " + datalist.get(6).size());
		String buffer = "";
		for(int i = 0; i < datalist.get(0).size(); i++)
		{
			int j = 0;
			for(; j < datalist.size() - 1; j++)
			{
				if(datalist.get(j).get(i).contains("</"))
				{
					buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') ) + "\",";
				}
				else
				{
					buffer += "\"" + datalist.get(j).get(i) + "\",";
				}
			}
			if(datalist.get(j).get(i).contains("</"))
			{
				buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') );
			}
			else
			{
				buffer += "\"" + datalist.get(j).get(i);
			}
			buffer += "\"\n";
		}
		System.out.print(buffer);
		bufWriter.write(buffer);
	}
}
