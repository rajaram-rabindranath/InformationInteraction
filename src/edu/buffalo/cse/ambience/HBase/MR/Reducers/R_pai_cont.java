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
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.math.Information;


public class R_pai_cont extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	private static final byte[] colfam=Bytes.toBytes(AMBIENCE_tables.mutualInfo.getColFams()[0]);
	private static final byte[] qual=Bytes.toBytes("PAI");
	ImmutableBytesWritable jobStatsT;
	ImmutableBytesWritable sinkT; 
	 
	int reducerID =0;
	static int numkeys=0;
	/*
	HashBag combo =new HashBag(); -- FIXME
	HashBag combo_n_target =new HashBag();
	HashBag target =new HashBag(); 
	-- reuse these object for GC overhead limit exceeded issues
	*/
	
	/**
	 * 
	 */
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		Configuration conf = context.getConfiguration();

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
		// debug code to check
		if(key.toString().equals("map"))
		{
			Put put;
			byte[] colfam_=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[0]);
			/*mapLogV=mapperID+","+numRecords;*/
			for(Text val:values)
			{
				String strVal=val.toString();
				String[] splits=strVal.split(",");
				put=new Put(Bytes.toBytes(splits[0]));// key
				put.add(colfam_,Bytes.toBytes("#recs"),Bytes.toBytes(splits[1]));
				context.write(jobStatsT, put);
			}
			context.progress();
			return;
		}
		
		HashMap<String,HashBag> mixedBag=new HashMap<String,HashBag>();
		HashBag combo =new HashBag();
		HashBag target =new HashBag();
		StringBuilder comboVal=new StringBuilder();
		
		String strComboVal;
		int rowz=0;
		int cnt=0;
		HashBag b;
		/**************val eg.****************
		 * -->A|B|C|trait 1_2_1_0.45,1
		 * -->A|B|C|trait 1_2_1_0.457,1
		 ************************************/
		for(Text val : values) 
        {   
        	comboVal.setLength(0);
        	String[] split = val.toString().split(",");
        	cnt = Integer.valueOf(split[1]);
        	String[] c =split[0].split(Constants.VAL_SPLIT);
        	for(int i =0;i<c.length-1;i++)
        	{
        		comboVal.append(c[i]);
        		comboVal.append(Constants.VAL_SEP);
        	}
        	
        	strComboVal=comboVal.toString();
        	/** combo **/
        	combo.add(comboVal.toString(),cnt);
        	/** target **/
        	target.add(c[c.length-1],cnt);
        	/** combo and target **/
        	if(mixedBag.containsKey(comboVal.toString()))
        		mixedBag.get(strComboVal).add(c[c.length-1],cnt);
        	else
        	{
        		b=new HashBag();b.add(c[c.length-1],cnt);
        		mixedBag.put(strComboVal,b);
        	}
        	rowz+=cnt;
        } 
		
		System.out.println("=============================="+key+"============================");
		if(key.toString().equals("0|1|2"))
		{
			//debug(mixedBag);
		    debug(target);
		    debug(combo);
		}
    	double PAI = Information.PAI(mixedBag,combo,target,rowz);
    	Put put = new Put(Bytes.toBytes(key.toString()));
    	put.add(colfam,qual,Bytes.toBytes(Double.toString(PAI)));
    	numkeys++;
    	context.write(sinkT, put);
    	context.progress();
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		Put put=new Put(Bytes.toBytes(Integer.toString(reducerID)));
		byte[] colfam=Bytes.toBytes(AMBIENCE_tables.jobStats.getColFams()[1]);
		byte[] qual=Bytes.toBytes("#numkeys");
		put.add(colfam,qual,Bytes.toBytes(Integer.toString(numkeys)));
		context.write(jobStatsT, put);
		context.progress();
	}
	
	/**
	 * At this point we need this only for -------
	 * 
	 * @param bag
	 * @param rows
	 * @param cols
	 * @return
	 */
	/*public double[][] targetMat(HashBag bag,int rows)
	{
		double[][] mat=new double[rows][1];
		Set<String> vals = bag.uniqueSet();
		int rowCnt=0;
		int cnt=0;
		for(String val:vals)
		{	
			cnt=bag.getCount(val);
			for(int i=rowCnt;i<rowCnt+cnt;i++)
				mat[i][0]=Double.valueOf(val);
			rowCnt+=cnt;
		}
		return mat;
	}*/
	
	public void debug(HashBag n)
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
	}
}
