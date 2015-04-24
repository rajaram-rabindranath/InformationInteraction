package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.math.Information;


public class R_kwiiList extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	private static final byte[] colfam=Bytes.toBytes("information_metric");
	private static final byte[] qual=Bytes.toBytes("PAI");
	static final ImmutableBytesWritable jobStatsT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.jobStats.getName()));
	static final ImmutableBytesWritable sinkT = new ImmutableBytesWritable(Bytes.toBytes(AMBIENCE_tables.mutualInfo.getName()));
	int reducerID =0;
	static int numkeys=0;
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		reducerID= context.getTaskAttemptID().getTaskID().getId();
		//Configuration conf = context.getConfiguration();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		/********************** FOMULA FOR THE PAI*************************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 *  			PAI(KEY)		= H(combo)	  + H(target) - H(combo_n_target)		
    	 ******************************************************************************/
		HashMap<String,Integer> combo=new HashMap<String,Integer>();
		HashMap<String,Integer> combo_n_target=new HashMap<String,Integer>();
		HashMap<String,Integer> target=new HashMap<String,Integer>();
		
		StringBuilder comboVal=new StringBuilder();
		StringBuilder targetVal=new StringBuilder();
		int count=0;
		
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
        	if (combo_n_target.containsKey(split[0]))
        		combo_n_target.put(split[0], combo_n_target.get(split[0]) + cnt);
    		else 
	        	combo_n_target.put(split[0],cnt);
	        /** combo **/
        	if(combo.containsKey(comboVal.toString()))
        		combo.put(comboVal.toString(),combo.get(comboVal.toString())+cnt);
        	else
        		combo.put(comboVal.toString(),cnt);
        	/** target **/
        	if(target.containsKey(targetVal.toString()))
        		target.put(targetVal.toString(),target.get(targetVal.toString())+cnt);
        	else
        		target.put(targetVal.toString(),cnt);
        	count+=cnt;
        }
    	double PAI = Information.PAI(combo, target, combo_n_target, count);
    	Put put = new Put(Bytes.toBytes(key.toString()));
    	put.add(colfam,qual,Bytes.toBytes(Double.toString(PAI)));
    	numkeys++;
    	context.write(sinkT, put);
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
	
	/*public void debug(HashMap<String,Integer> n)
	{
		{
	        System.out.println("********************************************");
	        int num=0;
	        Set<String> uniq = n.keySet();
	        for(String cell_coord : uniq)
	        {
	        	System.out.println(cell_coord+" : "+n.get(cell_coord));
	        	num+=Integer.valueOf(n.get(cell_coord));
	        }
	        System.out.println("Total is ::"+num);
        }
	}*/
	
	/*        System.out.println("=============================="+key+"============================");
    debug(combo_n_target);
    debug(target);
    debug(combo);
*/        
}
