package edu.buffalo.cse.ambience.HBase.MR.Reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.math.Math_;


public class R_entropy extends TableReducer<Text,Text, ImmutableBytesWritable>
{
	private static final byte[] colfam_cont=Bytes.toBytes("contingency");
	private static final byte[] colfam_info=Bytes.toBytes("infoMet");
	private static final byte[] qual_ent=Bytes.toBytes("ent");
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
		HashMap<String,Integer> combo=new HashMap<String,Integer>();
		int total=0;
		int cnt=0;
		double entropy=0.0f;
		/************************************
		 * val eg. -->A|B|C 1_2_1,1
		 ************************************/
		for(Text val : values) 
        {   
        	String[] split = val.toString().split(",");
        	cnt = Integer.valueOf(split[1]);
        	if(combo.containsKey(split[0]))
        		combo.put(split[0],combo.get(split[0])+cnt);
        	else
        		combo.put(split[0],cnt);
        	total+=cnt;
        }
		
		/** store data in contingency table **/
		Set<String> comboKeys=combo.keySet();
		Put put = new Put(Bytes.toBytes(key.toString()));
		byte[] qual_cont;
		for(String keys : comboKeys) // storing contingency information
		{
			qual_cont=Bytes.toBytes(keys);
			put.add(colfam_cont,qual_cont,Bytes.toBytes(Integer.toString(combo.get(keys))));
		}
		entropy = Math_.entropy(combo,total);
		put.add(colfam_info,qual_ent,Bytes.toBytes(Double.toString(entropy)));
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
