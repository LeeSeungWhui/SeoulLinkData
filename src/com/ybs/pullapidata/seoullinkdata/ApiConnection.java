package com.ybs.pullapidata.seoullinkdata;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

public class ApiConnection
{
	private String Url;
	private String KeyName;
	private String ServiceKey;
	private String BaseDate;
	private String BaseTime;
	private String Nx;
	private String Ny;
	private String PageNo;
	public StringBuilder urlBuilder;	
	Document result ;
	Elements elements;
	
	public List<String> getResult(String attribute) {
		List<String> resultArray = new ArrayList<String>();
		elements = result.select(attribute);
		for (Element e : elements) 
		{
           resultArray.add(e.text());
        }
		
		return resultArray;
	}

	public String getServiceKey() {
		return ServiceKey;
	}

	public void setServiceKey(String keyName, String serviceKey) throws UnsupportedEncodingException {
		KeyName = keyName;
		ServiceKey = serviceKey;
		urlBuilder.append("?" + URLEncoder.encode(keyName, "UTF-8") + "=" + serviceKey);
	}
	
	public String getUrl() {
		return Url;
	}

	public void setUrl(String url) {
		Url = url;
		urlBuilder = new StringBuilder(Url);
	}

	public String getBaseDate() {
		return BaseDate;
	}

	public void setBaseDate(String baseDate) {
		BaseDate = baseDate;
	}

	public String getBaseTime() {
		return BaseTime;
	}

	public void setBaseTime(String baseTime) {
		BaseTime = baseTime;
	}

	public String getNx() {
		return Nx;
	}

	public void setNx(String nx) {
		Nx = nx;
	}

	public String getNy() {
		return Ny;
	}

	public void setNy(String ny) {
		Ny = ny;
	}

	public String getPageNo() {
		return PageNo;
	}

	public void setPageNo(String pageNo) {
		this.PageNo = pageNo;
	}
	
	public void makeUrlBuilder() throws IOException
	{
		urlBuilder.append("?" + URLEncoder.encode(KeyName, "UTF-8") + "=" + ServiceKey);
		urlAppender("base_date", BaseDate);
		urlAppender("base_time", BaseTime);
		urlAppender("nx", Nx);
		urlAppender("ny", Ny);
		urlAppender("numOfRows", "10");
		urlAppender("pageNo", PageNo);
		urlAppender("_type", "xml");
	}
	
	public void urlAppender(String param, String val) throws IOException
	{
		urlBuilder.append("&" + URLEncoder.encode(param, "UTF-8") + "=" + URLEncoder.encode(val, "UTF-8")); 
	}
	
	public void pullData()
	{
		try {
			result = Jsoup.connect(new String(urlBuilder)).maxBodySize(0).parser(Parser.xmlParser()).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pullData();
		}
	}
	
	public void addData()
	{
		try {
			result.appendChild(Jsoup.connect(new String(urlBuilder)).maxBodySize(0).parser(Parser.xmlParser()).get());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pullData();
		}
	}
	
	ApiConnection()
	{
		/* blank */
	}
}
