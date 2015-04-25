package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;

public abstract class AbstractMapper extends TableMapper<Text,Text>
{
	static long numRecords=0;
	protected String INVALID=null;
	protected int k=0,n=0; // yes ..you guessed it right -- n choose k
	protected String TARGET=null;
	protected String src_cf[]=null;
	protected int mapperID=0;
	protected String mapLogK;
	protected String mapLogV;
	protected ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		// just for checking debug
		String factor =conf.get("mapreduce.task.io.sort.factor");
		System.out.println("The map io sort factor "+factor);
		
		mapperID= context.getTaskAttemptID().getTaskID().getId();
		TARGET=conf.get(MRParams.TARGET_VAR.toString());
		INVALID=conf.get(MRParams.INVALID_VALUE.toString());
		src_cf=AMBIENCE_tables.source.getColFams();
		try
		{
			k=Integer.valueOf(conf.get(MRParams.K_WAY.toString()));
			n=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
		}
		catch(NumberFormatException nex)
		{
			nex.printStackTrace();
			System.out.println("either the value of N or K or both are bad!!");
			throw nex;
		}
	}
	
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException 
    {
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
    	Set<byte[]> keys=origMap.keySet();
		int position=0;
		for(byte[] key:keys) // transform KV pairs
		{
			rowMap.put(position,new String(origMap.get(key)));
			position++;
		}
		if(rowMap.size()!=n) // debug
    		System.out.println("COLS # does not match --- please chck "+rowMap.size()+" | "+n);
    	rows.add(rowMap);
    	numRecords++;
    }
}
