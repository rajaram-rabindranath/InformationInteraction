package edu.buffalo.cse.ambience.dataStructures;

import java.util.ArrayList;

public class gyan
{
	String combination=null; // will refer to ids as stored in the db
	int korder=0;
	Combination comb;
	public double value=0.0f; 
	public byte[] orderedB=null;
	public ArrayList<Integer> comboList=new ArrayList<Integer>();
	public ContingencyT ctbl; 
	String delim=Constants.COMB_SPLIT;
	
	/**
	 * @param combination
	 */
	public gyan(String combination)
	{
		this.combination=combination;
		comb=new Combination(this.combination); 
		if(combination!=null)
		{
			for(String s : combination.split(delim))
				comboList.add(Integer.valueOf(s));
			korder=comboList.size();	
		}
		else
			korder=0;
	}
	
	/**
	 * 
	 * @param combination
	 * @param value
	 */
	public gyan(String combination,String delim,double value)
	{
		this.combination=combination;
		this.delim=delim;
		if(combination!=null)
		{
			for(String s : combination.split(delim))
				comboList.add(Integer.valueOf(s));
			korder=comboList.size();	
		}
		else
			korder=0;
		this.value=value;
	}
	
	/**
	 * 
	 * @param combination
	 * @param value
	 */
	public gyan(String combination,double value)
	{
		this(combination);
		this.value=value;
	}
	
	/**
	 * 
	 * @return
	 */
	public int getKorder()
	{
		return korder;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getCombination()
	{
		return combination;
	}
	
	/**
	 * 
	 * @param ctbl
	 */
	public void setContingencyTbl(ContingencyT ctbl)
	{
		this.ctbl=ctbl;
	}
	
	/**
	 * 
	 * @return
	 */
	public ContingencyT getContingencyTbl()
	{
		return ctbl;
	}
}