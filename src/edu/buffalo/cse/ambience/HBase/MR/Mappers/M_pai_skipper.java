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

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class M_pai_skipper extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,nCols=0;
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private String mapLogK;
	private String mapLogV;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	private ArrayList<String> targets = new ArrayList<String>();
	public static int iter=0;
	@Override
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
			nCols=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
		}
		catch(NumberFormatException nex)
		{
			nex.printStackTrace();
			System.out.println("either the value of N or K or both are bad!!");
			throw nex;
		}
	}
	
	/**
	 * Just a collector of records
	 */
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException
    {
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
		if(origMap.size()<k) return;
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		String targetValue =
				new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
		int[] colMap=new int[origMap.keySet().size()]; // index will give actual colID
    	
    	/**
    	 * Need 2 maps:
    	 * ROW MAP & COL MAP 
    	 */
    	Set<byte[]> keys=origMap.keySet();
		int position=0;
		for(byte[] key:keys) // transform KV pairs
		{
			//colMap[position]=Integer.valueOf(Bytes.toInt(bytes)(key));
			colMap[position]=Bytes.toInt(key);
			rowMap.put(position,new String(origMap.get(key)));
			position++;
		}
		StringBuilder strKey=new StringBuilder();
		StringBuilder strValue=new StringBuilder();
		
		int comb[]=new int[k];
		for(int i=0;i<k;i++)comb[i]=i; // feeder combination
		int nActual= rowMap.size();
		int index=0;
		do{
			// clearing key and value
			iter++;
			strKey.setLength(0);
			strValue.setLength(0);
			
			for(int i=0;i<k-1;i++)
			{
				strValue.append(rowMap.get(comb[i]));strValue.append(Constants.VAL_SEP);
				strKey.append(colMap[comb[i]]);strKey.append(Constants.COMB_SEP);
			}
			strValue.append(rowMap.get(comb[k-1]));strValue.append(Constants.VAL_SEP);
			strKey.append(colMap[comb[k-1]]);
			strValue.append(targetValue);
			
			context.write(new Text(strKey.toString()),new Text(strValue.toString()+",1"));
			context.progress();
			
			/** get next combination **/
			index=k-1;
			++comb[index];
			while ((index > 0) && (comb[index] >= nActual - k + 1 + index)) // check if the index is always > 0
			{
				--index;
				++comb[index];
			}
			if(comb[0] > nActual - k)
				break; 
			for(index = index + 1; index < k; ++index)
				comb[index] = comb[index - 1] + 1;
		}while(true);
		numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		System.out.println("Total number of records processed");
		mapLogK="map";
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(mapLogK),new Text(mapLogV));
	}
}