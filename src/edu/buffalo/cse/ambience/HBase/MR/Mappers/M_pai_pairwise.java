package edu.buffalo.cse.ambience.HBase.MR.Mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class M_pai_pairwise extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,n=0;
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private String mapLogK;
	private String mapLogV;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	private ArrayList<String> targets = new ArrayList<String>();
	
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
			n=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
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
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException 
    {
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
    	NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
    	Set<byte[]> keys=origMap.keySet();
		
		for(byte[] key:keys) // transform KV pairs
			rowMap.put(Bytes.toInt(key),new String(origMap.get(key)));
		if(rowMap.size()!=n) // debug
    		System.out.println("COLS # does not match --- please chck "+rowMap.size()+" | "+n);
    	rows.add(rowMap);
    	targets.add(targetValue);
    	numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/********************** FOMULA FOR THE PAI*****************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 **********************************************************************/
		int[] comb= new int[k];
    	for(int i=0;i<k;i++)
    		comb[i] = i;
    	StringBuilder strValue = new StringBuilder();
    	HashMap<Integer, String> curRow=null;
    	String target,val;
		HashBag miniCombiner=new HashBag();
		Text txtKey=new Text(),txtVal=new Text();
		long iter=0;
		if(numRecords != rows.size())
		{
			System.out.println("We have a fundamental problem");
			System.out.println("\n\n\n");
		}
		//key = new Text(Long.toString(iter)); -- FIXME use combination id instead of key -- so that splits can be made of the sink
		/**************************************************
		 * For each pairwise -- 1 ... N
		 * 	For each row
		 * 		Generate K,V pairs with outcome
		 * 	END FOR
		 * 		EMIT KV pairs
		 * END FOR
		 * 
		 **************************************************/
		for(int colID=0;colID<n;colID++) // for each variable
		{
			txtKey.set(Integer.toString(colID));
			miniCombiner.clear();
			for(int index=0;index<rows.size();index++)
			{
				strValue.setLength(0);
				curRow=rows.get(index);
				target=targets.get(index);
				val=curRow.get(colID);
				if(val.equals(INVALID))continue; // move to next row
				strValue.append(val);
				strValue.append(Constants.VAL_SEP);
				strValue.append(target);
				miniCombiner.add(strValue.toString());
			}	 
			// emit kv pairs
			for(String v:(Set<String>)miniCombiner.uniqueSet())
			{
				txtVal.set(v+","+miniCombiner.getCount(v));
				context.write(txtKey,txtVal);
			}
		}
		mapLogK="map";
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(mapLogK),new Text(mapLogV));
	}
}