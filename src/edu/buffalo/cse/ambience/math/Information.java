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
		/*System.out.println("combo "+Math_.entropy(combo,rowCnt));
		System.out.println("target "+Math_.entropyCont_norm(targetMat,rowCnt));
		System.out.println("combo n target "+Math_.entropyMixed_norm(mixedBag,rowCnt));
		System.out.println("The pai value is  "+PAI);*/
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
	 * 
	 * @param vars
	 * @param famBytes
	 * @param qualBytes
	 * @return
	 * @throws Exception
	 *//*
	private static ArrayList<Get> getComponents(String[] vars,byte[] famBytes,byte[] qualBytes) throws Exception
	{
		ArrayList<Get> queryRowList = new ArrayList<Get>();
		int K = vars.length;
		Vector src = new Vector<String>();
		Vector out = new Vector<String>();
		for(String s:vars)
		{
			src.add(s);
		}
		
		for(int k=1;k<=K;k++)
		{
			Vector all_snp_combs = new Vector();
			nchoosek(src,k,null,all_snp_combs);
			int snp_comb_count = all_snp_combs.size();
			for(int i=0;i<snp_comb_count;i++)
			{
				Vector snp_comb = (Vector)all_snp_combs.get(i);
				StringBuffer s = new StringBuffer("");
				for(int j=0;j<snp_comb.size()-1;j++)
				{
					s.append(snp_comb.get(j));
					s.append(Constants.COMB_SEP);
				}
				s.append(snp_comb.get(snp_comb.size()-1));
				Get rowInfo= new Get((Bytes.toBytes(s.toString())));
				rowInfo.addColumn(famBytes, qualBytes);
				queryRowList.add(rowInfo);
			}
		}
		return queryRowList;
	}*/
	
	/**
	 * 
	 * @param comb
	 */
	/*public static double KWII(String combo) throws Exception
	{
		String[] vars = combo.split(Constants.COMB_SPLIT);
		boolean invalid =false;
		int K= vars.length;
		double accumulator=0.0f;
		double kwii = 0.0f;
		String colQual="entropy_discrete";
		String colFam="info_interaction_met";
		byte[] qualBytes=Bytes.toBytes(colQual);
		byte[] famBytes=Bytes.toBytes(colFam);
		
		*//**
		 *  fetch the components of the equation
		 *  K-1 to 1 -- ways
		 *//*
		List<Get> queryRowList=getComponents(vars,famBytes, qualBytes);
		HBase_lib instance = HBase_lib.getInstance();
		HTable table = instance.getTableHandler("mutual_information");
		try
		{
			System.out.println("QueryList size "+queryRowList.size());
			Result[] results = table.get(queryRowList);
			int k=-99;
			int check=0;
			String key=null;
			for (Result r : results) 
            {
            	key = new String(r.getRow());
        		check=key.split(Constants.COMB_SPLIT).length;
        		if(k != check) // condition A
            	{
        			if(k==-99) 
        			{
        				k=check;
        			}
        			else
        			{
	            		double term = accumulator;
	        			if((K-k)%2==1)
	        				term = -accumulator;
	        			kwii += term;
	        			accumulator=0.0f; // reset accumulator
	        			System.out.println("KWII VALUE AT THIS STAGE "+kwii+"::"+k+"_"+K);
	        			k=check;
        			}
            	}
        		System.out.println(key+":"+Bytes.toDouble(r.getValue(famBytes, qualBytes)));
            	accumulator+=Bytes.toDouble(r.getValue(famBytes, qualBytes));
            }
			
			// for the last set -- we will not hit -- condition A (Look Above)
			double term = accumulator;
			if((K-k)%2==1)
				term = -accumulator;
			kwii += term;
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			System.out.println("WE have a problem!");
		}
		if(invalid) return -99.0f;
		kwii = -kwii;
		System.out.println("The final KWII value "+ kwii);
		return kwii;
	}*/	
	
	/*public static double KWII(Vector<StringBuffer> subsets,Iterable<Text> values)
	{
		double kwii=0.0f;
		for(StringBuffer key: subset_keys)
		{
			k = key.split(Constants.COMB_SPLIT).length;
			
			System.out.println("Computing the entropy of ="+key);
			ent = Math_.entropy(subsets.get(key));
			
			//System.out.println(" the key is "+ key +" ent="+ent);
			
			// component sign
			if((K-k)%2==1)
				ent = -ent;
			kwii +=ent;
		}
		
		kwii = -kwii;
		System.out.println("The final KWII value "+ kwii);
		return kwii;
		return kwii;
	}*/
	
	/**
	 * @param Combo
	 * @param ComboStats
	 * @param subsets
	 * @return
	 * @throws IOException
	 */
	/*public static double  KWII(String combo, HashMap<String,Integer> comboStats,HashMap<String,comboStats> subsets) throws IOException
	{
		double kwii=0.0f;
		
		*//** example
		 * KWII(A,B,C)= -[H(A)+H(B)+H(C)]+[H(AB)+H(BC)+H(AC)]-H(ABC) 
		 *//*
		int K=combo.split(Constants.COMB_SPLIT).length;
		int k=0;
		double ent=0.0f;
		ent = Math_.entropy(comboStats);
		kwii +=ent;
		Set<String> subset_keys=subsets.keySet(); 
		
		for(String key: subset_keys)
		{
			k = key.split(Constants.COMB_SPLIT).length;
			ent = Math_.entropy(subsets.get(key));
			if((K-k)%2==1)
				ent = -ent;
			kwii +=ent;
			//System.out.println("Computing the entropy of ="+key);
			//System.out.println(" the key is "+ key +" ent="+ent);
			// component sign
		}
		kwii = -kwii;
		return kwii;
	}*/
	
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