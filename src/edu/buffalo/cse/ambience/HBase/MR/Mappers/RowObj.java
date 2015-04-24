package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.util.HashMap;

public class RowObj 
{
	int colMap[];
	HashMap<Integer, String> rowMap;
	String targetVal;
	int colCnt;
	public RowObj( int colMap[],HashMap<Integer,String> rowMap,String targetVal)
	{
		this.colMap=colMap;
		this.rowMap=rowMap;
		this.targetVal=targetVal;
		colCnt=colMap.length;
	}
}
