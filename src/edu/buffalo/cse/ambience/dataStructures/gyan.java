package edu.buffalo.cse.ambience.dataStructures;

public class gyan
{
	public String combID;
	int korder;
	public String comb;
	public String[] arrCombID;
	public double value; // FIXME what is this value -- PAI/KWII what is it ?
	public byte[] orderedB;
	public gyan(String combID,double value)
	{
		arrCombID=combID.split(Constants.COMB_SPLIT);
		korder=arrCombID.length;
		this.combID=combID;
		this.value=value;
	}
	
	public gyan(String combID,String comb,double value)
	{
		arrCombID=combID.split(Constants.COMB_SPLIT);
		korder=arrCombID.length;
		this.combID=combID;
		this.comb=comb;
		this.value=value;
	}
}