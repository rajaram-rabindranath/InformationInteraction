package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.Set;

import orderly.DoubleWritableRowKey;
import orderly.Order;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.gyan;
import edu.buffalo.cse.ambience.math.Information;


public class R_contingency extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	private static final byte[] colfam=Bytes.toBytes(AMBIENCE_tables.contingency.getColFams()[0]);
	ImmutableBytesWritable jobStatsT;
	ImmutableBytesWritable sinkT; 
	ImmutableBytesWritable top;
	private locateT findT;
	int reducerID =0;
	HashBag combo_n_target =new HashBag();
	
	static int numkeys=0;
	
	/**
	 * 
	 */
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		Configuration conf = context.getConfiguration();
		
		String jobID = conf.get(MRParams.JOBID.toString());
		jobStatsT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()+jobID));
		sinkT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.contingency.getName()+jobID));;
		top=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.top.getName()+jobID));;
		
		//System.out.println("Time out string val"+conf.get("mapreduce.task.timeout"));
		// just for checking debug
		String factor =conf.get("mapreduce.task.io.sort.factor");
		//System.out.println("The map io sort factor "+factor);
		long timeout=0;
		conf.getLong("mapreduce.task.timeout",timeout);
		//System.out.println("The time out val according to getong is "+timeout);
	}
	
	/**
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		combo_n_target.clear();
		
		if(key.toString().equals(Constants.MAP_KEY)) // FIXME -- debug code -- remove this later
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
		
		/************************************
		 * val eg. -->A|B|C|trait 1_2_1_0,1
		 ************************************/
		for(Text val : values) 
        {   
        	String[] split = val.toString().split(",");
        	int cnt = Integer.valueOf(split[1]);
        	combo_n_target.add(split[0],cnt);
        }
		Put put = new Put(Bytes.toBytes(key.toString()));
    	for(String col: (Set<String>)combo_n_target.uniqueSet())
    	{
    		put.add(colfam,Bytes.toBytes(col),Bytes.toBytes(combo_n_target.getCount(col)));
    	}
    	numkeys++;
    	context.write(sinkT, put);
    	context.progress();
    }
	
	@Override
	/**
	 * 
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		byte[] colfam=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[1]);
		byte[] qual=Bytes.toBytes("#numkeys");
		put.add(colfam,qual,Bytes.toBytes(Integer.toString(numkeys)));
		context.write(jobStatsT, put);
		context.progress();
	}
}
