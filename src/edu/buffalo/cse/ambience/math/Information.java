package edu.buffalo.cse.ambience.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.dataStructures.*;


public class Information 
{
	
	
	/**
	 * 
	 * @param combo
	 * @param target
	 * @param combo_n_target
	 * @param count
	 * @return
	 * @throws IOException
	 */
	public static double PAI(HashMap<String,Integer> combo,HashMap<String,Integer> target,HashMap<String,Integer> combo_n_target,int count) throws IOException
	{
		double PAI=0.0f;
		PAI = Math_.entropy(combo,count) + Math_.entropy(target,count) - Math_.entropy(combo_n_target,count); 
		return PAI;
	}
	
	public static double PAI(HashBag combo,HashBag target,HashBag combo_n_target,int count) throws IOException
	{
		double PAI=0.0f;
		PAI = Math_.entropy(combo,count) + Math_.entropy(target,count) - Math_.entropy(combo_n_target,count); 
		return PAI;
	}
	
	public static double PAI(HashMap<String,HashBag> mixedBag,HashBag combo,HashBag target,int rowCnt)
	{
		double PAI=0.0f;
		double[][] targetMat=Math_.makeMatrix(target,rowCnt,1);
		PAI=Math_.entropy(combo,rowCnt)+Math_.entropyCont_norm(targetMat,rowCnt)-Math_.entropyMixed_norm(mixedBag,rowCnt);
		return PAI;
	}
	
	/**
	 * 
	 * @param combo
	 * @return
	 */
	public static double[] TCI_HELPER(String combo,HTable table) throws IOException
	{
		double TCI_[] =new double[2];
		String[] genes=combo.split(Constants.COMB_SPLIT);
		TCI_[0] = aggregateEntropy(genes,table);
		if(TCI_[0] == 0.0f)
		{
			return null;
		}
		String colQual="entropy_discrete";
		String colFam="info_interaction_met";
		byte[] qualBytes=Bytes.toBytes(colQual);
		byte[] famBytes=Bytes.toBytes(colFam);
		Get g=new Get(combo.getBytes());
		g.addColumn(famBytes,qualBytes);
		Result r=table.get(g);
		TCI_[1] =Bytes.toDouble(r.getValue(famBytes,qualBytes));
		return TCI_;
	}
	
	/**
	 * 
	 * @param genes
	 * @param table
	 * @return
	 * @throws IOException
	 */
	private static double aggregateEntropy(String[] genes,HTable table) throws IOException
	{
		double aggregate=0.0f;
		String colQual="entropy_discrete";
		String colFam="info_interaction_met";
		byte[] qualBytes=Bytes.toBytes(colQual);
		byte[] famBytes=Bytes.toBytes(colFam);
		ArrayList<Get> queryRowList = new ArrayList<Get>();
		for(String gene:genes)
		{
			System.out.println("genes"+gene);
			Get rowInfo= new Get((Bytes.toBytes(gene)));
			rowInfo.addColumn(famBytes, qualBytes);
			queryRowList.add(rowInfo);
		}
		Result[] results = table.get(queryRowList);
		System.out.println("SIZE :::"+results.length);
		for(Result r: results)
		{
			System.out.println(new String(r.getRow())+"::"+Bytes.toDouble(r.getValue(famBytes, qualBytes)));
			aggregate+=Bytes.toDouble(r.getValue(famBytes, qualBytes));
			System.out.println("agg::"+aggregate);
		}
		System.out.println("agg"+aggregate);
		return aggregate;
	}
	
	
	/**
	 * @param Combo
	 * @param ComboStats
	 * @param subsets
	 * @return
	 * @throws IOException
	 */
	public static double  KWII(int K, HashBag combinationStats,HashMap<int[],HashBag> subsets,int total) throws IOException
	{
		double kwii=0.0f;
		
		/*********************** SAMPLE 3 WAY ***********************
		 * KWII(A,B,C)= -[H(A)+H(B)+H(C)]+[H(AB)+H(BC)+H(AC)]-H(ABC) 
		 ************************************************************/
		int k=0;
		double ent=0.0f;
		ent = Math_.entropy(combinationStats,(double)total);
		kwii +=ent;
		Set<int[]> subset_keys=subsets.keySet(); 
		for(int[] key: subset_keys)
		{
			k=key.length;
			ent = Math_.entropy(subsets.get(key),(double)total);
			if((K-k)%2==1)
				ent = -ent;
			kwii +=ent;
		}
		
		kwii = -kwii;
		return kwii;
	}
	
	
	/**
	 * @param Combo
	 * @param ComboStats
	 * @param subsets
	 * @return
	 * @throws IOException
	 */
	public static double  KWII(int K, HashMap<String,Integer> comboStats,HashMap<int[],HashMap<String,Integer>> subsets,int total) throws IOException
	{
		double kwii=0.0f;
		
		/************************************************************
		 * KWII(A,B,C)= -[H(A)+H(B)+H(C)]+[H(AB)+H(BC)+H(AC)]-H(ABC) 
		 ************************************************************/
		int k=0;
		double ent=0.0f;
		ent = Math_.entropy(comboStats,(double)total);
		kwii +=ent;
		Set<int[]> subset_keys=subsets.keySet(); 
		for(int[] key: subset_keys)
		{
			k=key.length;
			ent = Math_.entropy(subsets.get(key),(double)total);
			if((K-k)%2==1)
				ent = -ent;
			kwii +=ent;
		}
		
		kwii = -kwii;
		return kwii;
	}
}


/*System.out.println("combo "+Math_.entropy(combo,rowCnt));
System.out.println("target "+Math_.entropyCont_norm(targetMat,rowCnt));
System.out.println("combo n target "+Math_.entropyMixed_norm(mixedBag,rowCnt));
System.out.println("The pai value is  "+PAI);*/