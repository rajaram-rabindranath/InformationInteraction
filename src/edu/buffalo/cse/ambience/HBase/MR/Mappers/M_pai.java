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

public class M_pai extends TableMapper<Text,Text>
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
		int position=0;
		for(byte[] key:keys) // transform KV pairs
		{
			rowMap.put(position,new String(origMap.get(key)));
			position++;
		}
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
    	StringBuilder strKey = new StringBuilder();
    	HashMap<Integer, String> curRow=null;
		HashBag miniCombiner=new HashBag();
		Text key=null;
		boolean esc=false;
		long iter=0;
		if(numRecords != rows.size())
		{
			System.out.println("We have a fundamental problem");
			System.out.println("\n\n\n");
		}
		
		//key = new Text(Long.toString(iter)); -- FIXME use combination id instead of key -- so that splits can be made of the sink
		
		/**************************************************
		 * For each combination in *N choose K*
		 * A. Create key
		 * B. iterate thru each row -- and generate values
		 **************************************************/
		int index;
		do
		{	
			iter++;
			strKey.setLength(0);
			miniCombiner.clear();
			for(int i=0;i<comb.length-1;i++)
			{
				strKey.append(comb[i]);
				strKey.append(Constants.COMB_SEP);
			}
			strKey.append(comb[comb.length-1]);
			key=new Text(strKey.toString());
			for(int rowIndex=0;rowIndex<rows.size();rowIndex++) // iterate thru rows for a given combination
			{ 
				curRow = rows.get(rowIndex);
				strValue.setLength(0);
				for(int i=0;i<comb.length;i++)
	        	{
					if(curRow.get(comb[i]).equals(INVALID))
	        		{
	        			esc=true;
	        			break;
	        		}
	        		strValue.append(curRow.get(comb[i]));
					strValue.append(Constants.VAL_SEP);
	        	}
				if(!esc)
	    		{
					strValue.append(targets.get(rowIndex));
					String val = strValue.toString();
					miniCombiner.add(val);
				}
				esc=false;
			}
			for(String val : (Set<String>)miniCombiner.uniqueSet())
				context.write(key,new Text(val+","+miniCombiner.getCount(val)));
			context.progress(); // for time out issues
			
			/** get next combination **/
			index=k-1;
			++comb[index];
			while ((index > 0) && (comb[index] >= n - k + 1 + index)) // check if the index is always > 0
			{
				--index;
				++comb[index];
			}
			if(comb[0] > n - k)//Combination (n-k, n-k+1, ..., n) reached
				break; 
			for(index = index + 1; index < k; ++index) //comb now looks like (..., x, n, n, n, ..., n).
				comb[index] = comb[index - 1] + 1; // Turn it into (..., x, x + 1, x + 2, ...)
		}while(true);
		mapLogK="map";
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(mapLogK),new Text(mapLogV));
	}
}