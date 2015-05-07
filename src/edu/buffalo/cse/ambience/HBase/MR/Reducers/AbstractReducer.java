package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.gyan;

public abstract class AbstractReducer extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	protected static final byte[] colfam=Bytes.toBytes(AMBIENCE_tables.mutualInfo.getColFams()[0]);
	protected static final byte[] qual=Bytes.toBytes("PAI");
	ImmutableBytesWritable jobStatsT;
	ImmutableBytesWritable sinkT; 
	ImmutableBytesWritable top;
	protected locateT findT;
	int reducerID =0;
	static int numkeys=0;
	
	/**
	 * 
	 */
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		Configuration conf = context.getConfiguration();
		findT=locateT.getInstance(10,order.top);
		
		System.out.println("Time out string val"+conf.get("mapreduce.task.timeout"));
		// just for checking debug
		String factor =conf.get("mapreduce.task.io.sort.factor");
		System.out.println("The map io sort factor "+factor);

		long timeout=0;
		
		conf.getLong("mapreduce.task.timeout",timeout);
		System.out.println("The time out val according to getong is "+timeout);
		String jobID = conf.get(MRParams.JOBID.toString());
		jobStatsT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()+jobID));
		sinkT=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.mutualInfo.getName()+jobID));;
		top=new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.top.getName()+jobID));;
	}
	
	@Override
	/**
	 * 
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		// committing top k vals
		byte[] colfam=Bytes.toBytes(AMBIENCE_tables.top.getColFams()[1]);
		byte[] qual=Bytes.toBytes("PAI");
		for(gyan g : findT.asList())
		{
			Put put=new Put(g.orderedB);
			put.add(colfam,qual,Bytes.toBytes(g.combID));
			context.write(top,put);
			context.progress();
		}
		
		// job stats committing
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		colfam=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[1]);
		qual=Bytes.toBytes("#numkeys");
		put.add(colfam,qual,Bytes.toBytes(Integer.toString(numkeys)));
		context.write(jobStatsT, put);
		context.progress();
	}
}
