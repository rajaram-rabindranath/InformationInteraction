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

public class M_kwii extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,n=0;
	private String src_cf[]=null;
	private int mapperID=0;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		// just for checking debug
		String factor =conf.get("mapreduce.task.io.sort.factor");
		mapperID= context.getTaskAttemptID().getTaskID().getId();
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
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
    	Set<byte[]> keys=origMap.keySet();
		int position=0;
		for(byte[] key:keys) // transform KV pairs
		{
			rowMap.put(position,new String(origMap.get(key)));
			position++;
		}
		rows.add(rowMap);
    	numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/*********************** SAMPLE 3 WAY ***********************
		 * KWII(A,B,C)= -[H(A)+H(B)+H(C)]+[H(AB)+H(BC)+H(AC)]-H(ABC) 
		 ************************************************************/
		int[] comb= new int[k];
    	for(int i=0;i<k;i++)
    		comb[i] = i;
    	StringBuilder strValue = new StringBuilder();
    	StringBuilder strKey = new StringBuilder();
    	HashMap<Integer, String> curRow=null;
		HashBag miniCombiner=new HashBag();
		Text txtKey=new Text();
		Text txtValue=new Text();
		boolean esc=false;
		long iter=0;
		if(numRecords != rows.size())
		{
			System.out.println("We have a fundamental problem");
			System.out.println("\n\n\n");
		}
		
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
			strValue.setLength(0);
			miniCombiner.clear();
			for(int i=0;i<comb.length-1;i++)
			{
				strKey.append(comb[i]);
				strKey.append(Constants.COMB_SEP);
			}
			strKey.append(comb[comb.length-1]);
			txtKey.set(strKey.toString());
			
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
					strValue.deleteCharAt(strValue.length()-1);
					miniCombiner.add(strValue.toString());
				}
				esc=false;
			}
			/** Emit K,V pairs **/
			for(String val : (Set<String>)miniCombiner.uniqueSet())
			{
				txtValue.set(val+","+miniCombiner.getCount(val));
				context.write(txtKey,txtValue);
			}
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
		
		// for statistics
		txtKey.set(Constants.MAP_KEY);txtValue.set(mapperID+","+numRecords+","+iter);
		context.write(txtKey,txtValue);
	}
}