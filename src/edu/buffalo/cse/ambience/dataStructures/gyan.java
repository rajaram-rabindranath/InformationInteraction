package edu.buffalo.cse.ambience.dataStructures;

import java.util.ArrayList;

public class gyan
{
	/**
	 * FIXME -- need a whole lot of getters and setters
	 */
	public String combID=null;
	public int korder=0;
	public String comb=null;
	public double value=0.0f; // FIXME what is this value -- PAI/KWII what is it ?
	public byte[] orderedB=null;
	public boolean isIDTranslated=false;
	public boolean isVARTranslated=false;
	public ArrayList<Integer> intCombID=new ArrayList<Integer>();
	
	/**
	 * @param combID
	 * @param value
	 */
	public gyan(String combID,double value)
	{
		this.combID=combID;
		if(combID!=null)
		{
			for(String s : combID.split(Constants.COMB_SPLIT))
				intCombID.add(Integer.valueOf(s));
			korder=intCombID.size();	
		}
		else
			korder=0;
		this.value=value;
		isIDTranslated=false;
		isIDTranslated=true;
	}

	
	/**
	 * 
	 * @param combID
	 * @param comb
	 * @param value
	 */
	public gyan(String combID,String comb,double value)
	{
		if(combID!=null)
		{
			for(String s : combID.split(Constants.COMB_SPLIT))
				intCombID.add(Integer.valueOf(s));
			korder=intCombID.size();	
		}
		else
			korder=0;
		isIDTranslated=true;
		isVARTranslated=true;
		this.combID=combID;
		this.comb=comb;
		this.value=value;
	}
	
	
	/**
	 * Have cpmbID and comb
	 * @param combID
	 * @param comb
	 */
	public gyan(String combID,String comb)
	{
		this.comb=comb;
		this.combID=combID;
		if(combID!=null)
		{
			for(String s : combID.split(Constants.COMB_SPLIT))
				intCombID.add(Integer.valueOf(s));
			korder=intCombID.size();	
		}
		else
			korder=0;
		this.isIDTranslated=true;
		this.isVARTranslated=true;
	}
}