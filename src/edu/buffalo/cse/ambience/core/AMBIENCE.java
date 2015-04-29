package edu.buffalo.cse.ambience.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;





import edu.buffalo.cse.ambience.parameters.CLI;
import edu.buffalo.cse.ambience.dataStructures.Columns;
import edu.buffalo.cse.ambience.dataStructures.Constants;
import edu.buffalo.cse.ambience.dataStructures.Rows;
import edu.buffalo.cse.ambience.dataStructures.Table;
import edu.buffalo.cse.ambience.dataStructures.VarClass;
import edu.buffalo.cse.ambience.database.LibHBase;

public abstract class AMBIENCE 
{
	public Scan s=null;
	private HashMap<MRParams, String> mrParams=new HashMap<MRParams,String>();
	private Table data=null;
	private ArrayList<String> varList=null;
	AMBIENCE_ops oper=AMBIENCE_ops.NONE;
	CLI cli=null;
	private Configuration hdfsConf;
	LibHBase HBase=null;
	
	public abstract boolean kwii(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean pai(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean entropy(Job job,String sinkT) throws IOException, InterruptedException,ClassNotFoundException;
	public abstract boolean all(Job job) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean kwiiList(Job job,String sinkT) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean skip(Job job,String sinkT) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean skipC(Job job,String sinkT) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean iter(Job job,String sinkT) throws IOException, InterruptedException, ClassNotFoundException;
	public abstract boolean start();
	
	String fname,mode,strVarList,jobID,Kway,reducerCnt,invalid,TopCombos,TopTOrder,Tvalue;
	int splitCnt;
	boolean haveVarList=false;
	
	public AMBIENCE(CLI cli,Configuration conf)
	{
		this.cli = cli;
		this.hdfsConf=conf;
		argsInit();
	}
	
	private void argsInit()
	{
		fname=cli.getFileName();
		splitCnt=Integer.valueOf(cli.getSplitsCnt());
		oper=AMBIENCE_ops.resolveOps(cli.getOperation());
		mode=cli.getMode();
		jobID=cli.getJobID();
		Kway=cli.getKway();
		reducerCnt=cli.getReducerCnt();
		invalid=cli.getInvalid();
		TopCombos=cli.getTopCombinations();
		TopTOrder=cli.getTopTOrder();
		Tvalue=cli.getTvalue();
		if(haveVarList=cli.hasVarList())
			strVarList=cli.getVarList();
	}
	
	public boolean bootup()
	{
		if(oper.equals(AMBIENCE_ops.NONE))
			return false;
		if(!readInput(fname,mode,Constants.DELIM_TAB)) return false;
		if(haveVarList)
		{
			varList=data.translate(strVarList,Constants.DELIM_COMMA);
			if(varList==null)
			{
				System.out.println("Input var list invalid!");
				return false;
			}
			data.setDictionary(varList);
			mrParams.put(MRParams.SET_SIZE,Integer.toString(varList.size()));
		}	
		else
			mrParams.put(MRParams.SET_SIZE,Integer.toString(data.getMRColsCnt()));
		
		mrParams.put(MRParams.JOBID,jobID);
		mrParams.put(MRParams.K_WAY,Kway);
		mrParams.put(MRParams.REDUCER_CNT,reducerCnt);
		mrParams.put(MRParams.INVALID_VALUE,invalid);
		mrParams.put(MRParams.TOP_T_CNT,Tvalue); 
		mrParams.put(MRParams.TOP_COMBINATIONS,TopCombos);
		mrParams.put(MRParams.METRIC_ORDER,TopTOrder);
		mrParams.put(MRParams.TARGET_VAR,data.getColumns().getTargetVar());
		
		AMBIENCE_tables sinkT=AMBIENCE_tables.getSinkT(oper);
		AMBIENCE_tables srcT=AMBIENCE_tables.source;
		AMBIENCE_tables jobStats=AMBIENCE_tables.jobStats;
		if(!dbSetup(cli.getJobID(),sinkT, srcT, jobStats))return false;
		
		setMRParams(HBase.getConf());
		return true;
	}
	
	
	private boolean dbSetup(String tblSuffix,AMBIENCE_tables sink,AMBIENCE_tables src,AMBIENCE_tables jobStats)
	{
		
		String srcTblname=src.getName()+tblSuffix;
		String sinkTblname=sink.getName()+tblSuffix;
		String jobStatsTblname=jobStats.getName()+tblSuffix;
		String[] srcColFams=src.getColFams();
		String topKTblname=AMBIENCE_tables.topPAI.getName()+tblSuffix; 
		
		if(oper.equals(AMBIENCE_ops.ITER) || oper.equals(AMBIENCE_ops.SKIP) || oper.equals(AMBIENCE_ops.SKIPC))
			HBase.setNegValueFilter(s,mrParams.get(MRParams.INVALID_VALUE));
		
		HBase = LibHBase.getInstance(hdfsConf);
		s=HBase.getScanner(varList);
		if(!HBase.setupMapping(data.getColumns().c,tblSuffix)) return false;
		System.out.println("Mapping tables created!");
		if(!HBase.createTable(srcTblname,srcColFams,splitCnt))return false;
		System.out.println("SRC TABLE CREATED");
		if(!HBase.loadData(srcTblname,data.getColumns().c,data.getRows().r,srcColFams))return false;
		System.out.println("DATA LOADED");
		if(!HBase.createTable(jobStatsTblname,jobStats.getColFams()))return false;
		System.out.println("JOBSTATS TABLE CREATED");
		if(!HBase.createTable(topKTblname,AMBIENCE_tables.topPAI.getColFams()))return false;
		System.out.println("TOPK TABLE CREATED");
		if(!HBase.createTable(sinkTblname,sink.getColFams()))return false;
		System.out.println("SINK TABLE CREATED");
		dbSetupDebug();
		return true;
	}
	
	private void dbSetupDebug()
	{
		String srctname=AMBIENCE_tables.source.getName()+cli.getJobID();
		String colFam=AMBIENCE_tables.source.getColFams()[0];
		try{
			HBase.displayRegionInfo(srctname);}
		catch(IOException iex){System.out.println("problem in displayin region info!!"); iex.printStackTrace();}
		System.out.println("----------------- SOME DEBUG DATA ---------------");
		System.out.println("Number of columns for the MR jobs is "+data.getMRColsCnt());
		System.out.println("Total number of rows in Source are "+HBase.getRowCnt(srctname,colFam));
		System.out.println("----------------- SOME DEBUG DATA ---------------");
		System.out.println("\n\n\n");
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
			//Job job = new Job(conf,oper.toString());
			Job job = Job.getInstance(conf,oper.toString());
			job.setNumReduceTasks(Integer.valueOf(mrParams.get(MRParams.REDUCER_CNT))); // FIXME -- need to make this configurable
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
				case SKIP:
					s.addFamily(Bytes.toBytes(src_cf[1])); // add the targetVar family
					skip(job, sinkT);
					break;
				case SKIPC:
					s.addFamily(Bytes.toBytes(src_cf[1])); // add the targetVar family
					skipC(job, sinkT);
					break;
				case ITER:
					s.addFamily(Bytes.toBytes(src_cf[1])); // add the targetVar family
					iter(job, sinkT);
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
				
				String sinkTblName=AMBIENCE_tables.getSinkT(oper).getName()+cli.getJobID();
				//HBase.topT("topT4",Integer.valueOf(cli.getTcount()),Integer.valueOf(cli.getKway()));
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
	private boolean readInput(String fileName,String mode,String delimiter)
	{
		BufferedReader br = null;
		ArrayList<ArrayList<String>> rows  = new ArrayList<ArrayList<String>>();
		ArrayList<String> colNames = new ArrayList<String>();
		try
		{
			String line;
			if(mode == null || mode.equals("dist"))
			{
				/* for CCR - load */
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
			components = line.split(delimiter);
			if(components.length==0)return false;
			colNames = new ArrayList<String>(Arrays.asList(components));
			while ((line = br.readLine()) != null) 
			{
				components = line.split(delimiter);
				ArrayList<String> n = new ArrayList<String>();
				for(int i=0;i<components.length;i++)
					n.add(components[i]);
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
		data = Table.getInstance(new Columns(colNames), new Rows(rows));
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
			if(mrParams.get(property)!=null)
				conf.set(property.toString(),mrParams.get(property));
		}
	}
	
	/****************************************************************
	 * PRINTS OUT JOB DIAGNOSTICS INFORMATION -- TIME TAKEN 
	 * @param job
	 ****************************************************************/
	private void getDiagnostics(Job job) throws IOException, InterruptedException
	{
		long start = job.getStartTime();
		long end = job.getFinishTime();
		System.out.println("Another calculation of time taken "+(double)(end-start)/1000000);
		TaskReport[] mappers  = job.getTaskReports(TaskType.MAP);
		TaskReport[] reducers = job.getTaskReports(TaskType.REDUCE);
		
		double totMapTime=0.0f;
		double totRedTime=0.0f;
		int milli=1000;
		
		System.out.println("MAPPER TIMES -----------------");
		
		for(TaskReport map : mappers)
		{
			System.out.print("The map id "+map.getTaskId());
			long duration=map.getFinishTime()-map.getStartTime();
			System.out.println(" t taken "+(double)(duration)/milli+" secs");
			totMapTime+=duration;
			
		}
		System.out.println("Totl time takne by all mappers "+(double)(totMapTime)/milli + "secs\n");
		
		System.out.println("REDUCER TIMES -----------------");
		for(TaskReport red : reducers)
		{
			System.out.print("The red id "+red.getTaskId());
			long duration=red.getFinishTime()-red.getStartTime();
			System.out.println(" t taken "+(double)(duration)/milli+" secs");
			totRedTime+=duration;
		}
		System.out.println("Totl time taken by all reducers "+(double)(totRedTime)/milli + "secs\n");
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