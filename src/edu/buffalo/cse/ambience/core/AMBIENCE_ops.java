package edu.buffalo.cse.ambience.core;

public enum AMBIENCE_ops 
{
	PAI,
	KWII,
	T,
	CONT,
	ALL,
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
			System.out.println("The operations requested by the user is invalid");
			return NONE;
		}
	}
	
}
