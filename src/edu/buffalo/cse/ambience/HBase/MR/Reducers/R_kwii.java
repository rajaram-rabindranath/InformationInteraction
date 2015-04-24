package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE;
import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.math.Information;

public class R_kwii extends TableReducer<Text, Text, ImmutableBytesWritable>
{
	String targetVar=null;
	int k=0;
	String splitKeys;
	String splitComb;
	static int numkeys;
	static final byte[] colfam=Bytes.toBytes(AMBIENCE_tables.mutualInfo.getColFams()[0]);
	static final byte[] qual=Bytes.toBytes("KWII");
	static final ImmutableBytesWritable sinkT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.mutualInfo.getName()));
	static final ImmutableBytesWritable jobStatsT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()));
	int reducerID =0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		Configuration conf = context.getConfiguration();
		targetVar=conf.get(MRParams.TARGET_VAR.toString());
		try
		{
			k = Integer.valueOf(conf.get(MRParams.K_WAY.toString()));
		}
		catch(NumberFormatException nex)
		{
			nex.printStackTrace();
			throw nex;
		}
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		numkeys++;
		HashMap<String,Integer> kwayCounts = new HashMap<String,Integer>();
		HashMap<int [],HashMap<String,Integer>> subsets =  new HashMap<int[],HashMap<String,Integer>>();
		
		/**************************************************************
        * We have a k-way[key] combination -- we need
        * 1. <k choose c> combinations, where c varies from k-1 to 1
        * 2. Create scaffolding [subsets] to hold all the values
        * 3. Kick start counting by firing up a loop
        **************************************************************/
        for(int[] comb :AMBIENCE.kwiiSubsets(k))
        {
        	subsets.put(comb,new HashMap<String,Integer>());
        }
		Set<int[]> subsetKeys= subsets.keySet();
		String[] valSplits;
		String[] split;
		int total=0;
		int cnt=0;
		
		/***************************************************
		 * For each value --- do the following
		 * 1. collect <kway> statistics
		 * 2. collect subsets of <kway>'s statistics
		 ***************************************************/
		for(Text v:values)
		{
			split = v.toString().split(",");
			cnt=Integer.valueOf(split[1]);
			
			/** accumulating kway statistics **/
			if(kwayCounts.containsKey(split[0])) 
				kwayCounts.put(split[0],kwayCounts.get(split[0])+cnt);
			else
				kwayCounts.put(split[0],cnt);
			
			valSplits=split[0].split(Constants.VAL_SPLIT);
			/***************************************************************
			 * For each subset <key> of Kway --
			 * 1. Construct <value> 
			 * 2. Keep a running count of the <value> for the given <key> 
			 **************************************************************/
			for(int[] comb:subsetKeys)
			{
				StringBuilder val=new StringBuilder();
				try
				{
					for(int i : comb)
					{
						val.append(valSplits[i]);
						val.append(Constants.VAL_SEP);
					}
					
					HashMap<String,Integer> valMap=subsets.get(comb);
					String n=val.toString();
					if(valMap.containsKey(n))
						valMap.put(n, valMap.get(n)+cnt);
					else
						valMap.put(n,cnt);
				}
				catch(NumberFormatException nex)
				{
					nex.printStackTrace();
				}
			}
			total+=cnt;
		}
		double kwii = Information.KWII(k,kwayCounts,subsets,total);
    	Put put = new Put(Bytes.toBytes(key.toString()));
    	put.add(colfam,qual,Bytes.toBytes(Double.toString(kwii)));
    	context.write(sinkT, put);
    	numkeys++;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		byte[] colfam=Bytes.toBytes("RedKeys");
		byte[] qual=Bytes.toBytes("Count");
		put.add(colfam,qual,Bytes.toBytes(Integer.toString(numkeys)));
		context.write(jobStatsT, put);
	}
	
	/**
	 * For debug purposes
	 * @param key
	 * @param subsets.
	 * @param comboStats
	 * @param total
	 */
	public void printCombo(String key,HashMap<int[],HashMap<String,Integer>>subsets,HashMap<String, Integer>comboStats,int total)
	{
		System.out.println("=========================="+key+"============================");
		for(String s : comboStats.keySet())
		{
			System.out.println(s+":::"+comboStats.get(s));
		}
		for(int[] k:subsets.keySet())
		{
			String s="";
			for(int i=0;i<k.length;i++)
				s+=Integer.toString(k[i])+Constants.COMB_SEP;
			HashMap<String,Integer> stats=subsets.get(k);
			System.out.println(s+"::::::::::::");
			for(String s1:stats.keySet())
			{
				System.out.println(s1+"--"+stats.get(s1));
			}
		}
	}
}


