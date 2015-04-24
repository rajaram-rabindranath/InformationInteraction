package edu.buffalo.cse.ambience.core;

public enum AMBIENCE_ops 
{
	PAI,
	KWII,
	T,
	CONT,
	ALL,
	ENT,
	SKIP,
	SKIPC,
	ITER,
	NONE;
	public static AMBIENCE_ops resolveOps(String str)
	{
		try
		{
			return valueOf(str.toUpperCase());
		}
		catch(Exception ex)
		{
			return NONE;
		}
	}
	
}
