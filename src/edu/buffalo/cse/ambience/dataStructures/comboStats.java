package edu.buffalo.cse.ambience.dataStructures;

import java.util.HashMap;

/**
 * 
 * @author dev
 *
 */
public class comboStats
{
	public HashMap<String,Integer> value_counts;
	public double count;
	public double mean;
	public double variance;
	public int sum;
	
	public comboStats()
	{
		value_counts=new HashMap<String,Integer>();
		count=0.0f;
		mean=0.0f;
		sum=0;
		variance=0.0f;
	}
	
	/**
	 * 
	 * @param key
	 */
	public void increment(String key)
	{
		// existing key
		if(value_counts.containsKey(key))
		{
			value_counts.put(key, value_counts.get(key)+1);
		}
		else // first time add
		{
			value_counts.put(key,1);
		}
		count++;
	}
	
	/**
	 * 
	 * @param key
	 */
	public void add(String key)
	{
		value_counts.put(key,1);
		count++;
	}
}
