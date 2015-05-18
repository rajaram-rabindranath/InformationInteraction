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

import edu.buffalo.cse.ambience.core.AMBIENCE;
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
	
	/**
	 * When the outcome variables is discrete - this is used
	 * @param combo
	 * @param target
	 * @param combo_n_target
	 * @param count
	 * @return
	 * @throws IOException
	 */
	public static double PAI(HashBag combo,HashBag target,HashBag combo_n_target,int count) throws IOException
	{
		double PAI=0.0f;
		PAI = Math_.entropy(combo,count) + Math_.entropy(target,count) - Math_.entropy(combo_n_target,count); 
		return PAI;
	}
	
	/**
	 * When the outcome variable is continuous  -- we choose to use this
	 * @param mixedBag
	 * @param combo
	 * @param target
	 * @param rowCnt
	 * @return
	 */
	public static double PAI(HashMap<String,HashBag> mixedBag,HashBag combo,HashBag target,int rowCnt)
	{
		double PAI=0.0f;
		double[][] targetMat=Math_.makeMatrix(target,rowCnt,1);
		PAI=Math_.entropy(combo,rowCnt)+Math_.entropyCont_norm(targetMat,rowCnt)-Math_.entropyMixed_norm(mixedBag,rowCnt);
		return PAI;
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
	 * 
	 * @param ctbl
	 * @return
	 */
	public static double KWII(ContingencyT ctbl,int k) throws IOException
	{
		String[] valSplits;
		HashMap<int [],HashBag> subsets =  new HashMap<int[],HashBag>();
		StringBuilder val = new StringBuilder();
		HashBag combo=ctbl.combo;
		for(int[] comb :AMBIENCE.kwiiSubsets(k)) // creating subsets
        	subsets.put(comb, new HashBag());
		Set<int[]> subsetKeys=subsets.keySet();
		int cnt=0;
		for(String v:(Set<String>)combo.uniqueSet())
		{
			cnt=combo.getCount(v);
			valSplits=v.split(Constants.VAL_SPLIT);
			HashBag valBag;
			for(int[] comb:subsetKeys)
			{
				val.setLength(0);
				for(int i : comb)
				{
					val.append(valSplits[i]);
					val.append(Constants.VAL_SEP);
				}
				valBag=subsets.get(comb);
				valBag.add(val.toString(),cnt);
			}
		}
		return KWII(k, combo, subsets,ctbl.totl);
	}
	
	/**
	 * 
	 * @param ctbl
	 * @return
	 */
	public static double PAI(ContingencyT ctbl) throws IOException
	{
		
		return PAI(ctbl.combo, ctbl.target, ctbl.combo_n_target, ctbl.totl);
	}
	
	
	/**
	 * 
	 * @param ctbl
	 * @return
	 */
	public static double Entropy(ContingencyT ctbl)
	{
		return Math_.entropy(ctbl.combo,ctbl.totl);
	}
	
}