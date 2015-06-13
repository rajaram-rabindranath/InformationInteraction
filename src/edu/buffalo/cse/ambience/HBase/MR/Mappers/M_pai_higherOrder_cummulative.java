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

import com.google.common.collect.Sets;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Combination;
import edu.buffalo.cse.ambience.dataStructures.Constants;


public class M_pai_higherOrder_cummulative extends TableMapper<Text,Text>
{
	static long numRecords=0;
	private String INVALID=null;
	private int k=0,n=0; // yes ..you guessed it right -- n choose k
	private String TARGET=null;
	private String src_cf[]=null;
	private int mapperID=0;
	private String mapLogK;
	private String mapLogV;
	private ArrayList<HashMap<Integer,String>> rows = new ArrayList<HashMap<Integer,String>>();
	private ArrayList<String> targets = new ArrayList<String>();
	private ArrayList<ArrayList<Combination>> list= new ArrayList<ArrayList<Combination>>();
	private ArrayList<Combination> TopCombList=new ArrayList<Combination>();
	private int comboSize=0;
	private Text txtKey=new Text();
	private Text txtVal=new Text(); 
	StringBuilder strKey=new StringBuilder();
	StringBuilder strValue=new StringBuilder();
	ArrayList<Integer> common=new ArrayList<Integer>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException, NumberFormatException 
	{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		// just for checking debug
		String factor=conf.get("mapreduce.task.io.sort.factor");
		System.out.println("The map io sort factor "+factor);
		
		mapperID= context.getTaskAttemptID().getTaskID().getId();
		TARGET=conf.get(MRParams.TARGET_VAR.toString());
		INVALID=conf.get(MRParams.INVALID_VALUE.toString());
		src_cf=AMBIENCE_tables.source.getColFams();
		try
		{
			String commonCols=conf.get(MRParams.COMMON_VARIABLES.toString());
			if(commonCols!=null)
			{
				for(String a:commonCols.split(","))
					common.add(Integer.valueOf(a));
			}
			k=Integer.valueOf(conf.get(MRParams.K_WAY.toString()));
			n=Integer.valueOf(conf.get(MRParams.SET_SIZE.toString()));
			String[] topComb=conf.get(MRParams.TOP_COMBINATIONS.toString()).split(","); // FIXME -- DELIMITER
			if(topComb.length==0)
				System.out.println("PLEASE SHARE SOME TOP COMBINATION USING "+MRParams.TOP_COMBINATIONS);
			for(int i=0;i<topComb.length;i++) // read in all the combinations as an array
				TopCombList.add(new Combination(topComb[i]));
		}
		catch(NumberFormatException nex)
		{
			System.out.println("The combinations given are wrng!!!!"); // FIXME log4j
			nex.printStackTrace();
			throw nex;
		}		
	}
	
	/**
	 * collect rows here to process them later in the cleanup method
	 */
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException
    {
		HashMap<Integer,String> rowMap = new HashMap<Integer,String>();
		String targetValue =new String(values.getFamilyMap(Bytes.toBytes(src_cf[1])).get(Bytes.toBytes(TARGET)));
		NavigableMap<byte[],byte[]> origMap=values.getFamilyMap(Bytes.toBytes(src_cf[0]));
		
		// chkValidity(origMap) -- FIXME a very valid method
		ArrayList<Combination> operList=buildVals(origMap,TopCombList);   
		if(operList.isEmpty()) return;     	
		for(byte[] key:origMap.keySet()) 
    		rowMap.put(Bytes.toInt(key),Bytes.toString(origMap.get(key)));
		if(rowMap.size()!=n) // debug
    		System.out.println("COLS # does ot match --- please chck "+rowMap.size()+" | "+n);
		list.add(operList); 
		rows.add(rowMap);
		targets.add(targetValue);
		numRecords++;
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		System.out.println("Total number of records processed"+numRecords);
		HashMap<String,HashBag> miniCombiner=new HashMap<String,HashBag>();
		HashMap<Integer, String> curRow=null;
		HashBag b;
		//int pos;
		int iter=0; // FIXME -- to not break the code at reducer-- commiting mapper stats
		String val;
		String target;
		/**
		 * for i:1.....N
		 * 	for[each row]
		 * 		for[each combination]
		 * 			ADD TO <HashBag>
		 * 		END
		 * 	END
		 *  <DO_IN_MAPPER_COMBINING>
		 * END 
		 */
		for(int colID=0;colID<n;colID++) //ALL ROWS SHALL HAVE SAME # of rows / NOT!
		{
			if(common.contains(colID))
			{
				System.out.println(colID+"  is present");
				continue;
			}
			miniCombiner.clear();
			for(int index=0;index<rows.size();index++)//for each row
			{
				curRow=rows.get(index);
				target=targets.get(index);
				val=curRow.get(colID);
				if(val.equals(INVALID))continue; // move to next row
				for(Combination comb: list.get(index)) // for each combination
				{
					if(!comb.contains(colID)) // not redundant
	    			{ 
	    				//pos=comb.insertAtIndex(colID);
	    				comb.genKV(colID,val,strKey,strValue);
	    				strValue.append(target);
	    				if((b=miniCombiner.get(strKey.toString()))!=null)
	    					b.add(strValue.toString());
	    				else
	    				{
	    					b=new HashBag();b.add(strValue.toString());
	    					miniCombiner.put(strKey.toString(),b);
	    				}
	    			}	 
				}
			}
			for(String k : miniCombiner.keySet()) // emit KV pairs
			{
				b=miniCombiner.get(k);
				txtKey.set(k);
				for(String v:(Set<String>)b.uniqueSet())
				{
					txtVal.set(v+","+b.getCount(v));
					context.write(txtKey,txtVal);
				}
			}
		}
		mapLogK="map";
		mapLogV=mapperID+","+numRecords+","+iter;
		context.write(new Text(mapLogK),new Text(mapLogV));
	}
	
	/**
	 * Do not process a combination for this row if: NEEDED IF WE ARE FILTERING OUT COLS HAVING <INVALID> value
	 * 1: # of cols in row is < the # of elements top combo
	 * 2: # intersecting cols is <  # of elements in combo
	 * @param origMap
	 */
	private ArrayList<Combination> chkValidity(NavigableMap<byte[], byte[]> origMap)
	{
		ArrayList<Combination> n = new ArrayList<Combination>();
		Set<byte[]> keys=origMap.keySet();
		for(Combination c : TopCombList)
		{
			if(!(origMap.size() <= c.size) &&!(Sets.intersection(c.byteArrSet,keys).size() < c.size))
				n.add(c);
		}
		return n;
	}
	
	/**
	 * 
	 * @param origMap
	 * @param list
	 * @return
	 */
	private ArrayList<Combination> buildVals(NavigableMap<byte[],byte[]> origMap,ArrayList<Combination> topCombinations)
	{
		StringBuilder val = new StringBuilder(); // FIXME -- get rid of rows giving wrong combination
		ArrayList<Combination> qualified=new ArrayList<Combination>();
		boolean bad=false;
		for(Combination c : topCombinations)
		{
			Combination copy= new Combination(c.strC);
			val.setLength(0);
			for(int i=0;i<copy.size-1;i++)
			{
				copy.strVArr[i]=Bytes.toString(origMap.get(copy.byteCArr[i]));
				if(copy.strVArr[i].equals(INVALID))
				{
					bad=true;break;
				}
				val.append(copy.strVArr[i]);
				val.append(Constants.VAL_SEP);
			}
			if(bad) // FIXME -- when we don't filter -99
			{
				bad=false;continue;
			}	
			copy.strVArr[copy.size-1]=Bytes.toString(origMap.get(copy.byteCArr[copy.size-1]));
			if(copy.strVArr[copy.size-1].equals(INVALID)) continue;
			val.append(copy.strVArr[copy.size-1]);
			copy.strV=val.toString();
			qualified.add(copy);
		}
		return qualified;
	}
}