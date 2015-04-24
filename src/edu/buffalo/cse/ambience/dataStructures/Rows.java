package edu.buffalo.cse.ambience.dataStructures;

import java.util.ArrayList;

public class Rows 
{
	public ArrayList<ArrayList<String>> r;
	public Rows(ArrayList<ArrayList<String>> r)
	{
		this.r = r;
	}
	public int size()
	{
		return r.size();
	}
}
