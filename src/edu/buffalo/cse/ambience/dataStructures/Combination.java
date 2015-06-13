package edu.buffalo.cse.ambience.dataStructures;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class Combination 
{
	public String strC; // string version of combination -- 6|7|8
	public int[] intCArr; // int array version of combination -- int[]{6,7,8}
	public String[] strCArr; // String array version of combination -- String[]{"6","7","8"}
	public byte[][] byteCArr; // byte array version of combination -- byte[][]
	public String strV; // String version of value  
	public String[] strVArr; // String array version of value
	public int size; // # of elements in the combination
	public int max; // value of last element in combination AKA maximum column id
	public int min; // value of first element in combination AKA minimum column id
	public Set<Integer> elementSet;
	public Set<byte[]> byteArrSet; // FIXME -- helps when hbase columes are byte[] can compare
	
	/**
	 * 
	 * @param comb
	 * @throws NumberFormatException
	 */
	public Combination(String comb) throws NumberFormatException
	{
		strC=comb; // 
		strCArr = comb.split(Constants.COMB_SPLIT); // set string array version
		size=strCArr.length; // set size
		elementSet=new HashSet<Integer>(); // FIXME to intersect with 
		byteArrSet=new HashSet<byte[]>();
		intCArr=new int[size];byteCArr=new byte[size][];
		strVArr=new String[size];
		int index=0;
		for(String a : strCArr) // setting int array & byteArray versions
		{
			intCArr[index]=Integer.valueOf(a);
			byteCArr[index]=Bytes.toBytes(intCArr[index]);
			byteArrSet.add(byteCArr[index]);
			elementSet.add(intCArr[index]);
			index++;
		}
		min=intCArr[0];
		max=intCArr[size-1];
	}
	
	/**
	 * Returns String representation of the Combination
	 */
	public String toString()
	{
		return strC;
	}
	/**
	 * 
	 * @param A
	 * @return
	 */
	public boolean contains(int A)
	{
		if(A<min && A>max)return true;
		if(elementSet.contains(A))
			return true;
		return false;
	}
	
	/**
	 * 
	 * @param A
	 * @return
	 */
	private int getInsertIndex(int colID)
	{
		if(contains(colID))return Integer.MIN_VALUE; // cannot insert already present
		boolean leftAppend=false;
		int pos=0;
		// should append key ?
		if(colID>max || (leftAppend=colID<min)) 
		{
			if(leftAppend)
				return pos;
			else 
				return size;
		}
		else // should insert the key -- find where binaryInsert?
		{
			int lowerBound = 0;
			int upperBound = size - 1;
			int curIn = 0;
			while (true) 
			{
				curIn = (upperBound + lowerBound) / 2;
				if(intCArr[curIn] < colID) 
				{
					lowerBound = curIn + 1; // its in the upper
					if (lowerBound > upperBound)
						return curIn + 1;
				} 
				else 
				{
					upperBound = curIn - 1; // its in the lower
					if (lowerBound > upperBound)
						return curIn;
				}
			}
		}
	}
	
	/**
	 * 
	 * @param colID
	 * @return
	 */
	public boolean append(int colID)
	{
		int pos=getInsertIndex(colID);
		if(pos==Integer.MIN_VALUE) return false;
		remakeCombination(colID,pos);
		return true;
	}
	
	/**
	 * 
	 * @param colID
	 * @param pos
	 */
	private void remakeCombination(int colID,int pos)
	{
		StringBuilder newComb=new StringBuilder();
		if(pos==0) // left append
		{
			newComb.append(colID);
			newComb.append(Constants.COMB_SEP);
			newComb.append(strC);
		}
		else if(pos==size) // right append
		{
			newComb.append(strC);
			newComb.append(Constants.COMB_SEP);
			newComb.append(colID);
		}
		else // insert
		{
			for(int i=0;i<pos;i++)
			{
				newComb.append(strCArr[i]);newComb.append(Constants.COMB_SEP);
			}
			newComb.append(colID);
			newComb.append(Constants.COMB_SEP);
			for(int i=pos;i<size;i++)
			{
				newComb.append(strCArr[i]);newComb.append(Constants.COMB_SEP);
			}
			newComb.deleteCharAt(newComb.length()-1); // FIXME better way of doing this
		}

		strC=newComb.toString(); 
		strCArr = newComb.toString().split(Constants.COMB_SPLIT);
		size=strCArr.length;
		elementSet.add(colID);
		byteArrSet.add(Bytes.toBytes(colID));
		intCArr=new int[size];byteCArr=new byte[size][];
		strVArr=new String[size];
		int index=0;
		for(String a : strCArr) // setting int array & byteArray versions
		{
			intCArr[index]=Integer.valueOf(a);
			byteCArr[index]=Bytes.toBytes(intCArr[index]);
			index++;
		}
		min=intCArr[0];
		max=intCArr[size-1];
	}
	
	/**
	 * 
	 * @param colID
	 * @param val
	 * @param strKey
	 * @param strVal
	 */
	public void genKV(int colID,String val,StringBuilder strKey,StringBuilder strVal)
	{
		int pos=getInsertIndex(colID);
		strKey.setLength(0);
		strVal.setLength(0);
		if(pos==0) // left append
		{
			strKey.append(colID);
			strKey.append(Constants.COMB_SEP);
			strKey.append(strC);
			strVal.append(val);
			strVal.append(Constants.VAL_SEP);
			strVal.append(strV);
			strVal.append(Constants.VAL_SEP);
		}
		else if(pos==size) // right append
		{
			strKey.append(strC);
			strKey.append(Constants.COMB_SEP);
			strKey.append(colID);
			strVal.append(strV);
			strVal.append(Constants.VAL_SEP);
			strVal.append(val);
			strVal.append(Constants.VAL_SEP);
		}
		else // insert
		{
			for(int i=0;i<pos;i++)
			{
				strKey.append(strCArr[i]);strKey.append(Constants.COMB_SEP);
				strVal.append(strVArr[i]);strVal.append(Constants.VAL_SEP);
			}
			strKey.append(colID);strVal.append(val);
			strKey.append(Constants.COMB_SEP);strVal.append(Constants.VAL_SEP);
			for(int i=pos;i<size;i++)
			{
				strKey.append(strCArr[i]);strKey.append(Constants.COMB_SEP);
				strVal.append(strVArr[i]);strVal.append(Constants.VAL_SEP);
			}
			strKey.deleteCharAt(strKey.length()-1); // FIXME better way of doing this
		}
		return;
	}
}
