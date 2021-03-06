package edu.buffalo.cse.ambience.core;



public enum AMBIENCE_tables 
{
	mutualInfo("mutual_information",new String [] {"infoMet"}),
	contingency("contingency",new String[]{"k_way"}),
	source("table",new String[]{"indVars","targetVar"}), // should this be discrete vs continuous
	stats("stats",new String[]{"A","B"}),
	jobStats("jobStats",new String[]{"MapStats","RedStats"}),
	top("top",new String[]{"DATA"}),
	fwdMap("fwdMap",new String[]{"id","class"}),  
	revMap("revMap",new String[]{"name","class"});
	
	private String tableName;
	private String[] cf;
	
	private AMBIENCE_tables(String tableName,String[] cf)
	{
		this.tableName = tableName;
		this.cf=cf;
	}
	
	public String getName()
	{
		return tableName;
	}
	
	public String[] getColFams()
	{
		return cf;
	}
	
	
	/**
	 * FIXME a very bad function --- needs to be removed
	 * @param ops
	 * @return
	 */
	public static String getColFam(AMBIENCE_ops ops)
	{
		
		switch(ops)
		{
			case CONT:
				return AMBIENCE_tables.contingency.cf[0];
			case ALL:
				return null;
			case PAI:
			case SKIP:
			case SKIPC:
			case AMBI:
				return AMBIENCE_tables.mutualInfo.cf[0];
			case ITER:
				return AMBIENCE_tables.mutualInfo.cf[0];
			case KWII:
				return AMBIENCE_tables.mutualInfo.cf[0];
			default:
				return null;
		}
	}
	
	
	public static AMBIENCE_tables getSinkT(AMBIENCE_ops ops)
	{
		switch(ops)
		{
			case CONT:
				return AMBIENCE_tables.contingency;
			case ALL:
				//need to return multiple tables
				//return null;
			case SKIP:
			case SKIPC:
			case ITER:
			case T:
			case KWII:
			case AMBI:
			case PAI:
				return AMBIENCE_tables.mutualInfo;
			case NONE:
				return null;
			default:
				return null;
		}
	}
}