package edu.buffalo.cse.ambience.dataStructures;

import java.util.ArrayList;

public class Columns 
{
	public ArrayList<String> c;
	
	public Columns(ArrayList<String> c)
	{
		this.c=c;
	}
	
	public int size()
	{
		return c.size();
	}

	public String getTargetVar()
	{
		return c.get(c.size()-1);
	}
	
}
