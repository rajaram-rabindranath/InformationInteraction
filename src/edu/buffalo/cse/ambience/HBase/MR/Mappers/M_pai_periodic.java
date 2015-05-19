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
public class M_pai_periodic extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private int k=0,nCols=0;
	int colCntMax=0;
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private String mapLogK;
	private String mapLogV;
	private ArrayList<RowObj> rows = new ArrayList<RowObj>();
	private HashMap<String,HashBag> periodic=new HashMap<String,HashBag>();
	private static int FLUSH_INTERVAL=Constants.FLUSH_INTERVAL_DEFAULT;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String factor =conf.get("mapreduce.task.io.sort.factor");
		mapperID= context.getTaskAttemptID().getTaskID().getId();
		TARGET=conf.get(MRParams.TARGET_VAR.toString());
		src_cf=AMBIENCE_tables.source.getColFams();
		FLUSH_INTERVAL=Integer.valueOf(conf.get(MRParams.FLUSH_INTERVAL.toString()));
//		System.out.println("FLUSH INTERVAL-------------------------------------------------------------------- "+FLUSH_INTERVAL);
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
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException 
    {
		NavigableMap<byte[],byte[]> origRowMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
		if(origRowMap.size()<k) return; // cannot build 3way if I have only 2 variables
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
    	int[] colMap=new int[origRowMap.keySet().size()]; // index will give actual colID
    	Set<byte[]> keys=origRowMap.keySet();
		int position=0;
		for(byte[] key:keys) // transform KV pairs
		{
			colMap[position]=Bytes.toInt(key);
			rowMap.put(position,new String(origRowMap.get(key)));
			position++;
		}
		colCntMax=rowMap.size() > colCntMax ? rowMap.size():colCntMax;
		rows.add(new RowObj(colMap, rowMap, targetValue));
		if(rowMap.size()!=nCols)
			System.out.println("COLS # does not match --- please chck "+rowMap.size()+" | "+nCols);
    	numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		/********************** FOMULA FOR THE PAI*****************************
    	 *  calculating PAI(X1,X2,X3,P) = H(X1,X2,X3) + H(P) - H(X1,X2,X2,X3,P)
    	 **********************************************************************/
		StringBuilder strValue = new StringBuilder();
    	StringBuilder strKey = new StringBuilder();
    	HashMap<Integer, String> currRowMap=null;
    	int[] currColMap;
		HashMap<Text,HashBag> minicomboiner=new HashMap<Text,HashBag>();
		Text key=new Text();
		Text value=new Text();
		long iter=0;
		int periodicCnt=0;
		
		if(numRecords != rows.size()) // DEBUG STATMENT -- FIXME log4j
		{
			System.out.println("We have a fundamental problem");
			System.out.println("\n\n\n");
		}
		
		//key = new Text(Long.toString(iter)); -- FIXME use comboination id instead of key -- so that splits can be made of the sink
		
		/**************************************************
		 * For each comboination in *N choose K*
		 * iterate through <rows> and create K,V pairs
		 **************************************************/
		int[] combo= new int[k];
    	for(int i=0;i<k;i++) // seed combination
    		combo[i] = i;
    	
		int index;
		ArrayList<RowObj> blackList=new ArrayList<RowObj>();
		RowObj r;
		do{	
			iter++;
			periodicCnt++;
			//minicomboiner.clear();
			/** iterate through all rows for current combination **/
			for(int rowIndex=0;rowIndex<rows.size();rowIndex++) 
			{
				r=rows.get(rowIndex);
				// skipper
				if(combo[k-1]>=r.colCnt)/** should i not process this row for current combination?**/
				{
					if(combo[0] >= r.colCnt - k)/** black-list this row and continue with next row] **/ 
					{
						blackList.add(r);
						continue; 
					}
					else /** just continue, don't black-list this row yet, later maybe **/
						continue; 
				}
				// Make K,V Pairs
				currColMap=r.colMap;
				currRowMap=r.rowMap;
				/** yes! process THIS row for THIS combination **/
				strKey.setLength(0);
				strValue.setLength(0);
				for(int i=0;i<k-1;i++)
				{
					strValue.append(currRowMap.get(combo[i]));strValue.append(Constants.VAL_SEP);
					strKey.append(currColMap[combo[i]]);strKey.append(Constants.COMB_SEP);
				}
				strKey.append(currColMap[combo[k-1]]);
				strValue.append(currRowMap.get(combo[k-1]));strValue.append(Constants.VAL_SEP);
				strValue.append(r.targetVal);
				String val = strValue.toString();
				HashBag Bag;
				/** any opportunity to combine -- exploited **/
				if((Bag=periodic.get(strKey.toString()))==null)
				{
					Bag =new HashBag();Bag.add(val);
					periodic.put(strKey.toString(),Bag);
				}
				else
					Bag.add(val);
			} // end of iterating thru rows -- for a given combination
			
			/** emit K,V pairs **/
			if(periodicCnt == FLUSH_INTERVAL) // emit all the left over key value pairs
			{
				for(String k:periodic.keySet())
				{
					HashBag Bag=periodic.get(k);
					key.set(Bytes.toBytes(k));
					for(String v: (Set<String>)Bag.uniqueSet())
					{
						value.set(Bytes.toBytes(v+","+Bag.getCount(v)));
						context.write(key,value);
						context.progress(); // for time out issues
					}
				}
				periodicCnt=0;
				periodic.clear(); // clear periodic
			}
			
			
			/** remove black-listed row/s **/
			for(RowObj o:blackList)
				rows.remove(o);
			blackList.clear();
			/** get next combination **/
			index=k-1;
			++combo[index];
			while ((index > 0) && (combo[index] >= colCntMax - k + 1 + index)) 
			{
				--index;
				++combo[index];
			}
			if(combo[0] > colCntMax - k)
				break; 
			for(index = index + 1; index < k; ++index) 
				combo[index] = combo[index - 1] + 1;
			
		}while(true);  
		
		if(periodicCnt>0) // emit all the left over key value pairs
		{
			for(String k:periodic.keySet())
			{
				HashBag Bag=periodic.get(k);
				key.set(Bytes.toBytes(k));
				for(String v: (Set<String>)Bag.uniqueSet())
				{
					value.set(Bytes.toBytes(v+","+Bag.getCount(v)));
					context.write(key,value);
					context.progress(); // for time out issues
				}
			}
			periodic.clear();
		}
		
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(Constants.MAP_KEY),new Text(mapLogV));
	}
	
	/*public void emit(contextText key,Text value) --- FIXME function should enable lessing code complication
	{
		
	}*/
}