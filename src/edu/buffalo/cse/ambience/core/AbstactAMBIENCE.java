package edu.buffalo.cse.ambience.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.Table;
import edu.buffalo.cse.ambience.dataStructures.VarClass;
import edu.buffalo.cse.ambience.database.LibHBase;
import edu.buffalo.cse.ambience.parameters.CLI;

public abstract class AbstactAMBIENCE 
{
	enum Mode
	{
		local("local"),
		dist("dist");
		
		private String m;
		
		private Mode(String m)
		{
			this.m = m;
		}
	}
	public Scan s=null;
	private HashMap<MRParams, String> mrParams=new HashMap<MRParams,String>();
	private Table data=null;
	private static HashMap<Integer,String> VAR_MAP=null;
	private static final String DEFAULT_DELIMITER="\\|";
	private ArrayList<String> tablefilter=null;
	AMBIENCE_ops oper=AMBIENCE_ops.NONE;
	LibHBase HBase =null; 
	CLI cli=null;
	private Configuration ambienceConf;
	public abstract boolean kwii(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean pai(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean entropy(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean all(Job job) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean kwiiList(Job job,String sinkT) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean start();
	
	public AbstactAMBIENCE(CLI cli,Configuration conf)
	{
		this.cli = cli;
		this.ambienceConf=conf;
	}
	
	public boolean bootup()
	{
		if((oper = AMBIENCE_ops.resolveOps(cli.getOperation())).equals(AMBIENCE_ops.NONE))
		{
			System.out.println("The operations requested by the user is invalid");
   			return false;
		}
		
		// read data from the input file
		if(!readInput(cli.getFileName(),cli.getMode())) return false;
		if(cli.hasCombFlag())
		{
			if(!translateCombo(cli.getColFilter(),DEFAULT_DELIMITER)) return false;
			if(tablefilter!=null)
				for(String c : tablefilter)
					System.out.println("the filter cols "+c);
		}
		if(cli.hasCombFlag())
			data.setDictionary(tablefilter);
		LibHBase.setconf(ambienceConf);
		HBase = LibHBase.getInstance();
		try
		{
			int n = Integer.valueOf(cli.getSplitsCnt());
			HBase.setSplitsCnt(n);
		}
		catch(NumberFormatException nex)
		{
			nex.printStackTrace();
		}
		
		// set MR parameters
		mrParams.put(MRParams.JOBID,cli.getJobID());
		mrParams.put(MRParams.K_WAY,cli.getKway());
		mrParams.put(MRParams.REDUCER_CNT,cli.getReducerCnt());
		mrParams.put(MRParams.INVALID_VALUE,cli.getInvalid());
		mrParams.put(MRParams.TARGET_VAR,data.getColumns().getTargetVar());
		
		
		if(tablefilter==null)
			mrParams.put(MRParams.COMB_SETSIZE,Integer.toString(data.getMRColsCnt()));
		else
			mrParams.put(MRParams.COMB_SETSIZE,Integer.toString(tablefilter.size()));
		
		
		// db setup & MR param set
		if(HBase.DBsetup(AMBIENCE_tables.source, AMBIENCE_tables.getSinkT(oper),
				AMBIENCE_tables.jobStats,data.getColumns(),data.getRows(),cli.getJobID()))
			HBase.displayRegionInfo();
		else
			return false;
		
		s=HBase.getScanner(tablefilter);
		s.setCaching(HBase.getSrcRegionSize()); // FIXME -- better way to do this
		
		String srctname=AMBIENCE_tables.source.getName()+cli.getJobID();
		String colFam=AMBIENCE_tables.source.getColFams()[0];
		System.out.println("----------------- SOME DEBUG DATA ---------------");
		System.out.println("Number of columns for the MR jobs is "+data.getMRColsCnt());
		System.out.println("Total number of rows in Source are "+HBase.getRowCnt(srctname,colFam));
		System.out.println("----------------- SOME DEBUG DATA ---------------");
		System.out.println("\n\n\n");
		setMRParams(HBase.getConf());
		return true;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean executeJob()
	{
		Configuration conf=HBase.getConf();
		String[] src_cf =AMBIENCE_tables.source.getColFams();
		try
		{
			Job job = new Job(conf,oper.toString());
			//Job job = Job.getInstance(conf,oper.toString());
			job.setNumReduceTasks(Integer.valueOf(mrParams.get(MRParams.REDUCER_CNT))); // FIXME -- need to make this configurable
			//job.setJarByClass(AMBIENCE.class);
			String sinkT=AMBIENCE_tables.getSinkT(oper).getName();
			final long startMilli = System.currentTimeMillis();
			final long startNano = System.nanoTime();
			
			switch(oper) 
			{
				case PAI:
					s.addFamily(Bytes.toBytes(src_cf[1])); // add the targetVar family 
					pai(job,sinkT);
					break;
				case KWII:
					kwii(job,sinkT);
					break;
				case ALL:
					all(job);
					break;
				case T:
					kwiiList(job,sinkT);
					break;
				case CONT:
				case ENT:
					entropy(job,sinkT);
					break;
				default:
					return false;
			}
			final long estMilli = System.currentTimeMillis() - startMilli;
			final long estNano = System.nanoTime() - startNano;

			if(job.isSuccessful())
			{
				System.out.println("\n\n\n");
				System.out.println("JOB SUCCESS");
				System.out.println("\n\n\n");
			}
			else
			{
				System.out.println("\n\n\n");
				System.out.println("JOB FAILED");
				System.out.println("\n\n\n");
			}
			
			System.out.println("\n\n=====Total time taken for the operation is====");
			System.out.println("Using currMilli::"+estMilli+" ms");
			System.out.println("Using nanoTime::"+(double)estNano/1000000+" ms");
			
			if(job.isSuccessful())
			{
				/** get the job diagnostics **/
				getDiagnostics(job);
				//String jobStatsTblName=AMBIENCE_tables.jobStats.getName()+;
				String sinkTblName=AMBIENCE_tables.getSinkT(oper).getName()+cli.getJobID();
				/*long sinkRows =HBase.getRowCnt(sinkTblName,"infoMet");
				System.out.println("# records in sink "+sinkRows);*/
				HBase.printJobStats(cli.getJobID());//(jobStatsTblName,AMBIENCE_tables.jobStats.getColFams()[1],1);
				HBase.readTable(sinkTblName,AMBIENCE_tables.getColFam(oper));
				System.out.println("# regions in sink "+HBase.getRegions(sinkTblName).size());
			}
		}
		catch(InterruptedException iex)
		{
			iex.printStackTrace();
			System.out.println("Job was interrupted");
		}
		catch(ClassNotFoundException cnex)
		{
			cnex.printStackTrace();
			System.out.println("Class not found");
		}
		catch(IOException iex)
		{
			iex.printStackTrace();
			System.out.println("General IO exception");
		}
        return true;
	}
	
	/**
	 * 
	 * @return
	 */
	private boolean setVarClass()
	{
		return true;
	}
	
	/**
	 * 		
	 * @param id
	 * @return
	 */
	private VarClass getClass(int id)
	{
		return VarClass.Discrete;
	}
	
	/**
	 * 
	 * @param comb
	 * @return
	 */
	public VarClass[] getVarClass(String comb)
	{
		String[] vars=comb.split(Constants.COMB_SPLIT);
		VarClass[] n = new VarClass[vars.length];
		try
		{
			int i=0;
			for(String s : vars)
			{
				n[i]=getClass(Integer.valueOf(s));
				i++;
			}
		}
		catch(NumberFormatException nex)
		{
			System.out.println("Number format excetion !!");
			nex.printStackTrace();
			return null;
		}
		return n;
	}
	
	/**
	 * Reads the data from the input file "fileName"
	 * Have the data packed and sent across to the caller
	 * @param fileName
	 * @return
	 */
	private boolean readInput(String fileName,String mode)
	{
		BufferedReader br = null;
		ArrayList<ArrayList<String>> rows  = new ArrayList<ArrayList<String>>();
		ArrayList<String> columnNames = new ArrayList<String>();
		String delimiter ="\t";
		try
		{
			String line;
			if(mode == null || mode.equals("dist"))
			{
				/* for CCR - load */
				/**
				 * /projects/vipin/rajaramr/ambience
				 */
				String path= File.separator+"projects"+File.separator+"vipin"+File.separator+"rajaramr"+File.separator+"ambience"+File.separator+"input"+File.separator;
				File in = new File(path+fileName);
				br = new BufferedReader(new FileReader(in));
			}
			else if(mode.equals("local"))
			{
				/* local load */
				Path pt=new Path("hdfs://localhost:54310/input/"+fileName);
		        FileSystem fs = FileSystem.get(new Configuration());
		        br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			}
	        String[] components;
			line = br.readLine();
			components = line.split("\t");
			if(components.length==0)return false;
			columnNames = new ArrayList<String>(Arrays.asList(components));
			while ((line = br.readLine()) != null) 
			{
				components = line.split("\t");
				StringBuffer b = new StringBuffer();
				
				ArrayList<String> n = new ArrayList<String>();
				for(int i=0;i<components.length;i++)
				{
					b.append(components[i]);
					b.append(",");
					n.add(components[i]);
				}
				b.deleteCharAt(b.length()-1);
				rows.add(n);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.out.println("COULD NOT READ THE DATA FROM THE INPUT FILE");
			System.out.println("MAY BE THE FORMAT OF THE FILE IS WRONG!!!!");
			return false;
		}
		finally 
		{
			try
			{
				if (br != null)br.close();
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
		data = Table.getInstance(new Columns(columnNames), new Rows(rows));
		return true;
	}
	
	/**
	 * 
	 * @param ids
	 * @return
	 */
	private boolean translateCombo(String ids,String delimiter)
	{
		if(ids==null)
			return true;
		
		System.out.println("The ids are "+ids);
		ArrayList<String> filter=new ArrayList<String>();
		String[] ele =ids.split(delimiter);
		int indVarID=data.getColCount()-1;
		int check=-1;
		try
		{
			int colID=0;
			for(String e : ele)
			{
				colID = Integer.parseInt(e);
				if(colID == indVarID || colID < check) // check if the combination is in proper order
					return false;
				filter.add(data.getColName(colID));
				check=colID;
			}
		}
		catch(NumberFormatException nex)
		{
			System.out.println("The input given for columns are not valid");
			nex.printStackTrace();
		}
		
		if(filter.size()==0)
		{
			System.out.println("Could not translate cols given");
			return false;
		}
		tablefilter=filter;
		return true;
	}
	
	/**
	 * 
	 * @param params
	 */
	public void setMRParams(Configuration conf)
	{
		Set<MRParams> properties = mrParams.keySet();
		for(MRParams property:properties)
		{
			conf.set(property.toString(),mrParams.get(property));
		}
	}
	
	/****************************************************************
	 * PRINTS OUT JOB DIAGNOSTICS INFORMATION -- TIME TAKEN 
	 * @param job
	 ****************************************************************/
	private void getDiagnostics(Job job) throws IOException, InterruptedException
	{
		/*long start = job.getStartTime();
		long end = job.getFinishTime();
		System.out.println("Another calculation of time taken "+(double)(end-start)/1000000);
		TaskReport[] mappers  = job.getTaskReports(TaskType.MAP);
		TaskReport[] reducers = job.getTaskReports(TaskType.REDUCE);
		
		double totMapTime=0.0f;
		double totRedTime=0.0f;
		int oneMill=1000000;
		
		System.out.println("MAPPER TIMES -----------------");
		
		for(TaskReport map : mappers)
		{
			System.out.print("The map id "+map.getTaskId());
			long duration=map.getFinishTime()-map.getStartTime();
			System.out.println(" t taken "+(double)(duration)/oneMill+" secs");
			totMapTime+=duration;
			
		}
		System.out.println("Totl time takne by all mappers "+(double)(totMapTime)/oneMill + "secs\n");
		
		System.out.println("REDUCER TIMES -----------------");
		for(TaskReport red : reducers)
		{
			System.out.print("The red id "+red.getTaskId());
			long duration=red.getFinishTime()-red.getStartTime();
			System.out.println(" t taken "+(double)(duration)/1000000+" secs");
			totRedTime+=duration;
		}
		System.out.println("Totl time taken by all reducers "+(double)(totRedTime)/oneMill + "secs\n");*/
	}
	
	/**
	 * @param comb
	 * @param n
	 * @param k
	 * @return
	 */
	public static boolean nextCombination(int[] comb,int n, int k)
	{
		int i = k - 1;
		++comb[i];
		while ((i > 0) && (comb[i] >= n - k + 1 + i)) // check if the index is always > 0
		{
			--i;
			++comb[i];
		}

		if (comb[0] > n - k) /* Combination (n-k, n-k+1, ..., n) reached */
			return false; /* No more combinations can be generated */

		/* comb now looks like (..., x, n, n, n, ..., n).
		Turn it into (..., x, x + 1, x + 2, ...) */
		for (i = i + 1; i < k; ++i)
		{	
			comb[i] = comb[i - 1] + 1;
		}
		return true;
	 
	}
	
	
	
	/**
	 * Pack the input into a nice HashMap
	 */
	public static TreeMap<String, String> transform(NavigableMap<byte[],byte[]> rowMap)
	{
		TreeMap<String, String> rowMap_trans=new TreeMap<String,String>();
		Set<byte[]> keys=rowMap.keySet();
		for(byte[] key:keys)
		{
			rowMap_trans.put(new String(key),new String(rowMap.get(key)));
		}
		return rowMap_trans;
	}
	
	
	public static HashMap<Integer,String> basicTransform(NavigableMap<byte[],byte[]> rowMap)
	{
		HashMap<Integer,String> rowMap_trans = new HashMap<Integer,String>();
		Set<byte[]> keys=rowMap.keySet();
		int position =0;
		for(byte[] key:keys)
		{
			rowMap_trans.put(position,new String(rowMap.get(key)));
			position++;
		}
		return rowMap_trans;
	}
	
	/**
	 * 
	 * @param k
	 * @return
	 */
	public static ArrayList<int[]> kwiiSubsets(int n)
	{
		ArrayList<int[]> subsets = new ArrayList<int[]>();
		int comb[];
		for(int k=n-1;k>1;k--)
		{
			comb = new int[k];
			for(int i=0;i<k;i++)
				comb[i]=i;
			subsets.add(comb.clone());
			while(nextCombination(comb, n, k))
				subsets.add(comb.clone());
		}
		for(int i=0;i<n;i++)
			subsets.add(new int[]{i});
		
		return subsets;
	}
}
