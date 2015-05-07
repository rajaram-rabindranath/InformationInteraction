package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
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
	static ImmutableBytesWritable sinkT;
	static ImmutableBytesWritable jobStatsT;
	HashBag Kway = new HashBag();
	HashMap<int [],HashBag> subsets =  new HashMap<int[],HashBag>();
	StringBuilder val=new StringBuilder();
	int reducerID =0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		
		Configuration conf = context.getConfiguration();
		String jobID = conf.get(MRParams.JOBID.toString());
		
		sinkT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.mutualInfo.getName()+jobID));
		jobStatsT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()+jobID));
		try
		{
			k = Integer.valueOf(conf.get(MRParams.K_WAY.toString())); // tests have to made by executJob() in AMBIENCE.java -- FIXME
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
		Kway.clear();subsets.clear();// Need to maintain a pool of HashBags and reuse them
		
		
		// debug code to check
		if(key.toString().equals(Constants.MAP_KEY))
		{
			Put put;
			byte[] colfam_=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[0]);
			for(Text val:values)
			{
				String strVal=val.toString();
				String[] splits=strVal.split(",");
				put=new Put(Bytes.toBytes(splits[0]));// key
				put.add(colfam_,Bytes.toBytes("#recs"),Bytes.toBytes(splits[1]));
				put.add(colfam_,Bytes.toBytes("#iter"),Bytes.toBytes(splits[2]));
				context.write(jobStatsT, put);
			}
			context.progress();
			return;
		}
		/**************************************************************
        * We have a k-way[key] combination -- we need
        * 1. <k choose c> combinations, where c varies from k-1 to 1
        * 2. Create scaffolding [subsets] to hold all the values
        * 3. Kick start counting by firing up a loop
        **************************************************************/
        for(int[] comb :AMBIENCE.kwiiSubsets(k))
        {
        	//subsets.put(comb,new HashMap<String,Integer>());
        	subsets.put(comb, new HashBag());
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
			Kway.add(split[0],cnt);// accumulating kway statistics
			
			/***************************************************************
			 * For each subset <key> of Kway --
			 * 1. Construct <value> 
			 * 2. Keep a running count of the <value> for the given <key> 
			 **************************************************************/
			valSplits=split[0].split(Constants.VAL_SPLIT);
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
			total+=cnt;
		}
		double kwii = Information.KWII(k,Kway,subsets,total);
    	Put put = new Put(Bytes.toBytes(key.toString()));
    	put.add(colfam,qual,Bytes.toBytes(Double.toString(kwii)));
    	context.write(sinkT, put);
    	numkeys++;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		byte[] colfam=Bytes.toBytes("RedStats");
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


