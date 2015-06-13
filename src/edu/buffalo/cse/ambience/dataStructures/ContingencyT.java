package edu.buffalo.cse.ambience.dataStructures;

import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.hbase.util.Bytes;

public class ContingencyT 
{
	public HashBag combo_n_target; // FIXME visibility
	public HashBag combo;
	public HashBag target;
	public int totl;
	
	public ContingencyT(NavigableMap<byte[], byte[]> map)
	{
		Set<byte[]> keys= map.keySet();
		String key;
		int count=0;
		combo_n_target=new HashBag();
		combo=new HashBag();
		target=new HashBag();
		for(byte[] colKey : keys)
    	{
			key=Bytes.toString(colKey);
			count=Bytes.toInt(map.get(colKey));
			totl+=count;
			combo_n_target.add(key,count);
			int index = key.lastIndexOf(Constants.VAL_SPLIT);
			combo.add(key.substring(0, index),count);
			target.add(key.substring(index),count);
		}	
	}
	
	public void printCTable()
	{
		System.out.println("The combo");
		print(combo);
		System.out.println("The combo and target");
		print(combo_n_target);
		System.out.println("The target");
		print(target);
	}
	
	private void print(HashBag bag)
	{
		for(String val : (Set<String>)bag.uniqueSet())
		{
			System.out.println(val+"----"+bag.getCount(val));
		}
	}
}
