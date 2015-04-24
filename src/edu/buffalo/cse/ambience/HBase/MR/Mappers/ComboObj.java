package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import edu.buffalo.cse.ambience.dataStructures.Constants;



class ComboObj 
{
	public String strC;
	public int[] intCArr;
	public String[] strCArr;
	public byte[][] byteCArr;
	public String strV;
	public String[] strVArr;
	public int size;
	public int max;
	public int min;
	
	public boolean valid; // in the context of iterations
	public double[] infoMet; // many metrics
	public Set<Integer> intSet;
	public Set<byte[]> byteArrSet;
	public ComboObj(String comb) throws NumberFormatException
	{
		strC=comb;
		strCArr = comb.split(Constants.COMB_SPLIT);
		size=strCArr.length;
		byteArrSet=new HashSet<byte[]>(); /// FIXME to intersect with 
		intCArr=new int[size];byteCArr=new byte[size][];
		strVArr=new String[size];
		int index=0;
		for(String a : strCArr) 
		{
			intCArr[index]=Integer.valueOf(a);
			byteCArr[index]=Bytes.toBytes(intCArr[index]);
			byteArrSet.add(byteCArr[index]);
			index++;
		}
		intSet= new HashSet(Arrays.asList(intCArr));
		min=intCArr[0];
		max=intCArr[size-1];
	}
}
