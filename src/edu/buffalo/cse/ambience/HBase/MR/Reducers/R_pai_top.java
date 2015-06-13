package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.gyan;
import edu.buffalo.cse.ambience.math.Information;

public class R_pai_top extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	private static final int MAX_TOP=10;
	ImmutableBytesWritable jobStatsT;
	ImmutableBytesWritable sinkT; 
	ImmutableBytesWritable top;
	private locateT findT;
	int reducerID =0;
	static int numkeys=0;
	int numReducers=0;
	long tsSeed=0;
	/**
	 * 
	 */
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		int T=MAX_TOP;
		if(conf.get(MRParams.TOP_T_CNT.toString())!=null)
		{
			T=Integer.valueOf(conf.get(MRParams.TOP_T_CNT.toString()));
			findT=locateT.getInstance(T,order.top);
		}
		numReducers=context.getNumReduceTasks();
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		if(numReducers>1) // setting Time Stamp seed
			tsSeed=reducerID*T;
		String jobID = conf.get(MRParams.JOBID.toString());
		jobStatsT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()+jobID));
		sinkT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.mutualInfo.getName()+jobID));;
		top=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.top.getName()+jobID));;
	}

	/**
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		/********************** FOMULA FOR THE PAI*************************************
		 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
		 *  			PAI(KEY)		= H(combo)	  + H(target) - H(combo_n_target)		
		 ******************************************************************************/
		HashBag combo =new HashBag();
		HashBag combo_n_target =new HashBag();
		HashBag target =new HashBag();
		StringBuilder comboVal=new StringBuilder();
		StringBuilder targetVal=new StringBuilder();
		int count=0;

		// FIXME --- debug code to check for statistics gathering 
		if(key.toString().equals(Constants.MAP_KEY))
		{
			Put put;
			byte[] colfam_=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[0]);
			/*mapLogV=mapperID+","+numRecords+","+iter;*/
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

		/************************************
		 * val eg. -->A|B|C|trait 1_2_1_0,1
		 ************************************/
		for(Text val : values) 
		{   
			comboVal.setLength(0);
			targetVal.setLength(0);
			String[] split = val.toString().split(",");
			int cnt = Integer.valueOf(split[1]);
			String[] c =split[0].split(Constants.VAL_SPLIT);
			targetVal.append(c[c.length-1]);
			for(int i =0;i<c.length-1;i++)
			{
				comboVal.append(c[i]);
				comboVal.append(Constants.VAL_SEP);
			}
			/** combo and target **/
			combo_n_target.add(split[0],cnt);
			combo.add(comboVal.toString(),cnt);
			target.add(targetVal.toString(),cnt);
			count+=cnt;
		}
		
		/*if(key.toString().equals("0|1|10"))
		{
			System.out.println("=============================="+key+"============================");
			debug(combo_n_target);
			debug(target);
			debug(combo);
		}*/
		double PAI = Information.PAI(combo, target, combo_n_target, count);
		/** collect top T combinations **/
		findT.add(key.toString(),PAI);
		numkeys++;
		context.progress();
	}

	@Override
	/**
	 * 
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		// committing top k vals
		byte[] colfam=Bytes.toBytes(AMBIENCE_tables.top.getColFams()[0]);
		byte[] qual=Bytes.toBytes("ID");
		long timeStamp=tsSeed;
		for(gyan g : findT.asList())
		{
			if(numReducers==1) // single reducer use clocks
				timeStamp=System.nanoTime();
			Put put=new Put(g.orderedB,timeStamp);
			put.add(colfam,qual,Bytes.toBytes(g.getCombination()));
			context.write(top,put);
			context.progress();
			timeStamp++;
		}
		// job stats committing
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		colfam=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[1]);
		qual=Bytes.toBytes("#numkeys");
		put.add(colfam,qual,Bytes.toBytes(Integer.toString(numkeys)));
		context.write(jobStatsT, put);
		context.progress();
	}

	/*public void debug(HashBag n)
	{
		{
			System.out.println("********************************************");
			int num=0;
			Set<String> uniq = n.uniqueSet();
			for(String cell_coord : uniq)
			{
				System.out.println(cell_coord+" : "+n.getCount(cell_coord));
				num+=Integer.valueOf(n.getCount(cell_coord));
			}
			System.out.println("Total is ::"+num);
		}
	}*/
}
