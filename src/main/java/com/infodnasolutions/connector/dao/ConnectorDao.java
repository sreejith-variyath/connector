package com.infodnasolutions.connector.dao;

import org.apache.manifoldcf.core.database.DBInterfacePostgreSQL;
import org.apache.manifoldcf.core.interfaces.*;

import com.infodnasolutions.connector.constants.DatabaseConstants;

import java.util.HashMap;
import java.util.Map;



public class ConnectorDao extends DBInterfacePostgreSQL {

	/** The thread context */
	protected IThreadContext threadContext;

	public ConnectorDao(IThreadContext tc, String databaseName, String userName, String password) throws ManifoldCFException {
		super(tc, databaseName, userName, password);
		this.threadContext=tc;
	}


	@Override
	public void openDatabase() throws ManifoldCFException {
		super.openDatabase();
	}

	public void createDatabaseTable() throws ManifoldCFException {

		Map columnMap = new HashMap();
		columnMap.put(DatabaseConstants.ID,new ColumnDescription("SERIAL",true,false,null,null,false));
		columnMap.put(DatabaseConstants.FILE_NAME,new ColumnDescription("VARCHAR(255)",false,false,null,null,false));
		columnMap.put(DatabaseConstants.FILE_CREATED_DATE,new ColumnDescription("DATE",false,true,null,null,false));
		columnMap.put(DatabaseConstants.FILE_MODIFIED_DATE,new ColumnDescription("DATE",false,true,null,null,false));
		columnMap.put(DatabaseConstants.FILE_SIZE,new ColumnDescription("BIGINT",false,true,null,null,false));
		performCreate(DatabaseConstants.DATABASE_TABLE_NAME,columnMap,null);
		System.out.println("Table created successfully with name"+"\t"+DatabaseConstants.DATABASE_TABLE_NAME);
	}


	public void insertFileInfo(Map<String, Object> paramMap) throws ManifoldCFException {
		performInsert(DatabaseConstants.DATABASE_TABLE_NAME, paramMap, null);
		System.out.println(":Records inserted successfully:");
	}


	public void showAllFileInfo()throws ManifoldCFException {

		System.out.println("=============showAllFileInfo() start=============");
		IResultSet set = performQuery("select * from "+DatabaseConstants.DATABASE_TABLE_NAME+";",null,null,null);
		System.out.println("Total No of Rows:"+set.getRowCount());
		int i = 0;
		while (i < set.getRowCount()) {
			IResultRow row = set.getRow(i);
			System.out.println("ID="+row.getValue(DatabaseConstants.ID));
			System.out.println("FileName="+row.getValue(DatabaseConstants.FILE_NAME));
			System.out.println("FileCreatedDate="+row.getValue(DatabaseConstants.FILE_CREATED_DATE));
			System.out.println("FileSize="+row.getValue(DatabaseConstants.FILE_SIZE));
			System.out.println("FileModifiedDate="+row.getValue(DatabaseConstants.FILE_MODIFIED_DATE));
			i++;
		}
		System.out.println("=============showAllFileInfo() end=============");
	}


	public String[] getAllFileNames()  throws ManifoldCFException{

		IResultSet set = performQuery("select"+"\t" +DatabaseConstants.FILE_NAME+"\t" +"from" +"\t" +DatabaseConstants.DATABASE_TABLE_NAME+";",null,null,null);
		String[] results = new String[set.getRowCount()];
		int i = 0;
		while (i < results.length){	

			IResultRow row = set.getRow(i);
			results[i] = (String)row.getValue(DatabaseConstants.FILE_NAME);
			i++;
		}
		return results;
	}


	public String printValues(String[] values) {
		StringBuffer sb = new StringBuffer("{");
		int i = 0;
		while (i < values.length){
			if (i > 0)
				sb.append(",");
			sb.append(values[i++]);
		}
		sb.append("}");
		return sb.toString();
	}


	@Override
	public void closeDatabase() throws ManifoldCFException {
		super.closeDatabase();
	}


}
