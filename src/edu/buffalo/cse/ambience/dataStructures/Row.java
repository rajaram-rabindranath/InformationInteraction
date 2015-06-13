package edu.buffalo.cse.ambience.dataStructures;

import java.util.HashMap;

public class Row
{
	public int colMap[];
	public HashMap<Integer, String> rowMap;
	public String targetVal;
	public int colCnt;
	public Row( int colMap[],HashMap<Integer,String> rowMap,String targetVal)
	{
		this.colMap=colMap;
		this.rowMap=rowMap;
		this.targetVal=targetVal;
		colCnt=colMap.length;
	}
}
