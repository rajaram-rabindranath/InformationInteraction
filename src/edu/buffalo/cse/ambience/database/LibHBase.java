package edu.buffalo.cse.ambience.database;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import orderly.DoubleWritableRowKey;
import orderly.Order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.io.compress.Compression;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.core.MRParams;
import edu.buffalo.cse.ambience.dataStructures.Constants;

public class LibHBase implements DBOps
{
	private Configuration conf=null;
	private HBaseAdmin hAdmin=null;
	private Configuration hdfsConf=null;
	private static LibHBase instance=null;
	private static final int DEFAULT_SCANNER_CACHING=500;
	private boolean idem=false;
	private static final String[] alphabets={"A","B","C","D","E","F","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"};
	private static final int alphaLen=alphabets.length;
	private static String tblSuffix=""; //Reason -- MR jobs using the same nodes; eating into each others data
	private HashMap<String,HTable> tables=new HashMap<String,HTable>(); //HTable creation is costly 
	private static final long DEFAULT_TIMEOUT=1800000*2; 
	private static boolean shouldCompress=false;
	
	/**
	 * From experience -- FIXME
	 * @author dev
	 */
	private enum MRplatformParams
	{
		/*mapreduce.map.sort.spill.percent
		mapreduce.tasktracker.report.address
		mapreduce.reduce.shuffle.retry-delay.max.ms
		mapreduce.reduce.shuffle.connect.timeout
		mapreduce.reduce.shuffle.read.timeout
		mapreduce.reduce.shuffle.parallelcopies
		mapreduce.task.timeout
		mapreduce.map.cpu.vcores
		mapreduce.reduce.cpu.vcores
		mapreduce.reduce.shuffle.merge.percent
		mapreduce.reduce.shuffle.input.buffer.percent
		mapreduce.reduce.input.buffer.percent
		mapreduce.shuffle.max.connections
		mapreduce.job.speculative.slowtaskthreshold
		mapreduce.output.fileoutputformat.compress.type
		mapreduce.output.fileoutputformat.compress.codec
		mapreduce.map.output.compress.codec*/
	}
	
	private LibHBase(Configuration hdfsConf) throws MasterNotRunningException,ZooKeeperConnectionException,IOException
	{
		this.hdfsConf=hdfsConf;
		conf = HBaseConfiguration.create(hdfsConf);
		hAdmin = new HBaseAdmin(conf);
		setFrameWorkParams();
	}
	
	
	
	public Configuration getConf()
	{
		return conf;
	}
	
	private void setFrameWorkParams()
	{
		conf.setLong("mapreduce.task.timeout", DEFAULT_TIMEOUT);
		conf.setBoolean("mapreduce.map.speculative",false);
	}
	
	public void setCompression(boolean b)
	{
		shouldCompress=b;
	}
	
	/**
	 * 
	 * @param params
	 */
	public void setMRParams(HashMap<MRParams, String> mrParams)
	{
		Set<MRParams> properties = mrParams.keySet();
		for(MRParams property:properties)
		{
			if(mrParams.get(property)!=null)
				conf.set(property.toString(),mrParams.get(property));
		}
	}
	/**
	 * @return
	 */
	public static LibHBase getInstance(Configuration hdfsConf) 
	{
		if(instance == null)
		{
			try
			{
				instance = new LibHBase(hdfsConf);
				return instance;
			}
			catch(MasterNotRunningException mex)
			{System.out.println("Master not running!");mex.printStackTrace();}
			catch (ZooKeeperConnectionException zex) 
			{System.out.println("Zookeeper connx expection!");zex.printStackTrace();}
			catch (IOException ioex)
			{System.out.println("General exception!");ioex.printStackTrace();}
		}
		return instance;
	}
	
	
	/**
	 * @param milliseconds
	 */
	public void setTimeout(long milliseconds)
	{
		conf.setLong("mapreduce.task.timeout", milliseconds);
	}
	
	/**
	 * 
	 * @param suffix
	 */
	public void setTblSuffix(String suffix)
	{
		tblSuffix=suffix;
	}
	
	/**
	 * 
	 * @param serverCnt
	 * @return
	 */
	private byte[][] genSplits(int splitCnt)
	{
		byte[][] regionSplits=new byte[splitCnt-1][];
		for(int i=0;i<splitCnt-1;i++)
			regionSplits[i] = Bytes.toBytes(getRegionBoundary(i+1));
		return regionSplits;
	}
	
	/**
	 * 
	 * @param i
	 * @return
	 */
	private String getRegionBoundary(int i)
	{
		if(i > alphaLen-1)
			return (int)Math.ceil((double)(i/alphaLen))+alphabets[(alphaLen-1)-i%(alphaLen)];
		return alphabets[i];
	}
	
	/**
   	 * 
   	 * @param tableName
   	 * @return
   	 * @throws IOException
   	 */
   	public NavigableMap<HRegionInfo,ServerName> getRegions(String tableName) throws IOException,TableNotFoundException
   	{
   		HTable handle = getTableHandler(tableName);
   		return handle.getRegionLocations();
   	}
   	
   	/**
   	 * Given a set of variable names (combination) return their ids [in sorted order]
   	 * @param var
   	 * @return
   	 * @throws IOException
   	 */
	public ArrayList<String> getID(ArrayList<String> many,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		ArrayList<String> trans=new ArrayList<String>();
   		String rslt;
   		for(String t : many)
   		{
   			if((rslt=getID(t,delim))!=null)
   				trans.add(rslt);
   		}
   		return trans;
   	}
	
	/**
	 * 
	 * @param combination
	 * @param delim
	 * @return
	 * @throws IOException
	 * @throws NumberFormatException
	 */
   	public String getID(String combination,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		if(combination==null) return null;
   		String[] splits= combination.split(delim);
   		if(splits.length==1){System.out.println("INVALID DELIMITER FOR "+combination);}
   		String[] rslt=getID(splits);
   		StringBuilder b =new StringBuilder();
   		b.setLength(0);
		for(int i=0;i<rslt.length-1;i++)
		{
			b.append(rslt[i]);b.append(Constants.COMB_SEP);
		}
		b.append(rslt[rslt.length-1]);
		return b.toString();
   	}
   	
   	/**
   	 * 
   	 * @param var
   	 * @return
   	 * @throws IOException
   	 */
   	private String[] getID(String... var) throws IOException,NumberFormatException,ElementNotFoundException
	{
   		String fwdMap=AMBIENCE_tables.fwdMap.getName()+tblSuffix;
   		String[] fwdMapCF=AMBIENCE_tables.fwdMap.getColFams();
   		byte[] colfam=Bytes.toBytes(fwdMapCF[0]);
   		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
   		try
   		{
	   		HTable tbl=getTableHandler(fwdMap) ;
			String[] mapped=new String[var.length];
			Get g;
			for(int i=0;i<var.length;i++)
			{
				g = new Get(Bytes.toBytes(var[i]));
				mapped[i]=Bytes.toString(tbl.get(g).getValue(colfam,qual));
				if(mapped[i]==null) throw new ElementNotFoundException(); // if entity not present
			}
			sort(mapped);
			return mapped;
   		}
   		catch(TableNotFoundException tnfex)
   		{
   			System.out.println("Mapping tables not found!");
   			tnfex.printStackTrace();
   			throw new ElementNotFoundException();
   		}
	}
   	
   	
	/**
   	 * Given a set of ids returns variable names 
   	 * @param ids
   	 * @return
   	 * @throws IOException
   	 */
   	public ArrayList<String> getVar(ArrayList<String> many,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		ArrayList<String> trans=new ArrayList<String>();
   		for(String t : many)
   			trans.add(getVar(t,delim));
   		return trans;
   	}
   	
   	/**
   	 * 
   	 * @param combination
   	 * @param delim
   	 * @return
   	 * @throws IOException
   	 * @throws NumberFormatException
   	 */
   	public String getVar(String combination,String delim) throws IOException,NumberFormatException,ElementNotFoundException
   	{
   		if(combination==null) return null;
   		String[] splits= combination.split(delim);
   		if(splits.length==1){System.out.println("INVALID DELIMITER FOR "+combination);}
   		String[] rslt=getVar(splits);
   		
   		StringBuilder b =new StringBuilder();
   		b.setLength(0);
		for(int i=0;i<rslt.length-1;i++)
		{
			b.append(rslt[i]);b.append(Constants.COMB_SEP);
		}
		b.append(rslt[rslt.length-1]);
   		return b.toString(); 
   	}
   	
   	/**
   	 * 
   	 * @param ids
   	 * @return
   	 * @throws IOException
   	 */
   	private String[] getVar(String... ids) throws IOException,NumberFormatException,ElementNotFoundException
	{
   		String revMap=AMBIENCE_tables.revMap.getName()+tblSuffix;
   		String[] revMapCF=AMBIENCE_tables.revMap.getColFams();
   		byte[] colfam=Bytes.toBytes(revMapCF[0]);
   		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
   		try
   		{
	   		HTable tbl= getTableHandler(revMap) ;
	   		sort(ids);
			String[] mapped=new String[ids.length];
			Get g;
			for(int i=0;i<mapped.length;i++)
			{
				g = new Get(Bytes.toBytes(ids[i])); // FIXME --- use getRecord for this
				mapped[i]=Bytes.toString(tbl.get(g).getValue(colfam,qual));
				if(mapped[i]==null) throw new ElementNotFoundException(); // if entity not present
			}
			return mapped;	
   		}
   		catch(TableNotFoundException tnfex)
   		{
   			System.out.println("Mapping table not found!");
   			tnfex.printStackTrace();
   			throw new ElementNotFoundException();
   		}
	}
   	
   /**
   	 * Non MR way of counting table row count
   	 * @param tableName
   	 */
   	public long getRowCnt(String tableName,String colfam) throws TableNotFoundException
   	{
   		HTable table = null;
		long rowCnt=0;
		System.out.println("-------------- Table Row cnt ---------------");
		try
		{
			table = getTableHandler(tableName);
			Scan scan = new Scan();
			scan.setCaching(DEFAULT_SCANNER_CACHING); 
	
			scan.addFamily(Bytes.toBytes(colfam));
			ResultScanner scanner = table.getScanner(scan);
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
				rowCnt++;
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tableName);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing");
			}
		}
		return rowCnt;
	}
   	
   	/**
   	 * 
   	 * @return
   	 */
   	public ArrayList<String> getRegStartKeys(NavigableMap<HRegionInfo,ServerName> regions)
   	{
   		ArrayList<String> regionKeys=new ArrayList<String>();
   		Set<HRegionInfo> regSet = regions.keySet();
		for(HRegionInfo hr : regSet)
    	{
    		if(Bytes.toStringBinary(hr.getStartKey()).isEmpty()) // hack
    			regionKeys.add("0");
    		else
    			regionKeys.add(Bytes.toStringBinary(hr.getStartKey()));
    	}
		return regionKeys;
	}
   	
	/**
	 * 
	 * @param tableFilter
	 * @return
	 */
	public Scan getScanner(AMBIENCE_tables tbl)
	{
		Scan s = new Scan();
		String src_cf[] = tbl.getColFams();
		s.addFamily(Bytes.toBytes(src_cf[0]));
		s.setCaching(DEFAULT_SCANNER_CACHING); 
		s.setCacheBlocks(false); // always set false for MR jobs
		return s;
	}
	
	/**
	 * Trusting the caller to have the arrayList rightly populated 
	 * i.e. with Objects of the right dataTypes
	 * @param s
	 * @param cols
	 * @param operation
	 */
	public void setColsTo(Scan s, String[] cols,boolean operation) throws IOException,ElementNotFoundException
	{
		FilterList filterList = new FilterList();
		Filter qualfilter;
		CompareOp op= operation ? CompareOp.EQUAL:CompareOp.NOT_EQUAL;
		String[] colIDs=getID(cols);
		for(String colID:colIDs)
		{	
			qualfilter=new QualifierFilter(op,new BinaryComparator(Bytes.toBytes(colID)));
			filterList.addFilter(qualfilter);
		}
		s.setFilter(filterList);
		
	}
	
	public Scan getScanner(int cacheSize,Filter filter) // FIXME
	{
		return null;
	}
	
	public void setRejectVal(Scan s,String value)
	{
		Filter negFilter=new ValueFilter(CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(value)));
		s.setFilter(negFilter);
	}
	
	public void setAcceptVal(Scan s,String value)
	{
		Filter negFilter=new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
		s.setFilter(negFilter);
	}
	
	
	public void setColLimit(Scan s, int colLimt)
	{
		Filter filter = new ColumnCountGetFilter(colLimt);
		s.setFilter(filter);
	}
	
	/**
	 * 
	 * @return
	 */
	public Scan getSrcTblScan()
	{
		Scan s = new Scan();
		String src_cf[] = AMBIENCE_tables.source.getColFams();
		s.addFamily(Bytes.toBytes(src_cf[0]));
		s.setCaching(DEFAULT_SCANNER_CACHING); 
		return s;
	}
	
   	/**
   	 * 
   	 * @param tableName
   	 * @return
   	 */
   	public boolean tableExists(String tableName)
   	{
   		boolean retVal=false;
   		try
   		{retVal = hAdmin.tableExists(tableName);}
   		catch (IOException e)
   		{e.printStackTrace();}
   		return retVal;
   	}
   	
   	/**
	 * Create table in Hbase for storing data
	 */
	public boolean createTable(String tableName, String[] colfams) 
	{
		boolean retVal=true;
		HColumnDescriptor colDesc;
		try
		{
			if(tableExists(tableName))
			{
				hAdmin.disableTable(tableName);
				hAdmin.deleteTable(tableName);
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for(int i =0;i<colfams.length;i++)
			{
				colDesc=new HColumnDescriptor(colfams[i]);
				if(shouldCompress)colDesc.setCompressionType(Compression.Algorithm.LZO);
				tableDescriptor.addFamily(colDesc);
			}
			hAdmin.createTable(tableDescriptor);
			try
			{tables.put(tableName,getTableHandler(tableName));}
			catch(TableNotFoundException tnex)
			{tables.put(tableName,new HTable(conf, tableName));}
			
		}
		catch (IOException e)
   		{
   			System.out.println("IO exception !!");
   			e.printStackTrace();
   			retVal=false;
   		}
   		return retVal;
	}

	/**
	 * OverLoaded method
	 * @return
	 */
	public boolean createTable(String tableName, String[] colfams,int splitCnt) 
	{
		Boolean retVal= true;
		HColumnDescriptor colDesc;
		try
		{
			byte[][] splits=genSplits(splitCnt);
			if(tableExists(tableName))
			{
				hAdmin.disableTable(tableName);
				hAdmin.deleteTable(tableName);
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			
			for(int i =0;i<colfams.length;i++)
			{
				colDesc=new HColumnDescriptor(colfams[i]);
				if(shouldCompress)colDesc.setCompressionType(Compression.Algorithm.LZO); 
				tableDescriptor.addFamily(colDesc);
			}
			hAdmin.createTable(tableDescriptor,splits);
			try
			{tables.put(tableName,getTableHandler(tableName));}
			catch(TableNotFoundException tnex)
			{
				tnex.printStackTrace();
				return false;
			}
		}
		catch (IOException e)
   		{
   			System.out.println("IO exception !!");
   			e.printStackTrace();
   			retVal=false;
   		}
   		return retVal;
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public HTable getTableHandler(String tblname) throws TableNotFoundException
	{
		if(!tableExists(tblname)) // check if table exists also a variant of the name
		{
			tblname=tblname+tblSuffix;
			if(!tableExists(tblname))
				throw new TableNotFoundException();
		}
		HTable table = tables.get(tblname);
		try
		{
			if(table==null)
			{
				tables.put(tblname,new HTable(conf,tblname));
				return tables.get(tblname);
			}
		}
		catch(IOException e) 
		{
			e.printStackTrace();
		}
		return table;	
	}
	
	public boolean printJobStats(String jobID)
	{
		String tablename= AMBIENCE_tables.jobStats.getName()+jobID;
		String[] cf=AMBIENCE_tables.jobStats.getColFams();
		HTable table = null;
		try
		{
			table = getTableHandler(tablename);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.addFamily(Bytes.toBytes(cf[0]));
			System.out.println("------------- Mapper Stats ------------");
			ResultScanner scanner = table.getScanner(scan);
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				// handling mutual info table
				Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(cf[0]));
			    Set<byte[]> entry =  list.keySet();
			    System.out.print("MapperID="+key);
			    //#keys=4,#n=4,#rec=6
			    for(byte[] colKey : entry)
            	{
			    	System.out.print(Bytes.toString(colKey)+"="+Bytes.toString(list.get(colKey))+",");
			    }
			    System.out.println();
			}
			scan = new Scan();
			scan.setCaching(500);
			scan.addFamily(Bytes.toBytes(cf[0]));
			scan.addFamily(Bytes.toBytes(cf[1]));
			scanner = table.getScanner(scan);
			System.out.println("------------- Reducer Stats ------------");
			long acc=0;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				// handling mutual info table
				Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(cf[1]));
			    Set<byte[]> entry =  list.keySet();
			    System.out.print("ReducerID="+key);
			    for(byte[] colKey : entry)
            	{
			    	System.out.println(Bytes.toString(colKey)+"="+Bytes.toString(list.get(colKey))+",");
			    	acc+=Integer.valueOf(Bytes.toString(list.get(colKey)));
			    }
			}
			System.out.println("Total # of keys processed by reducer "+acc);
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tablename);
			return false;
		}
		catch(TableNotFoundException tnex)
		{
			tnex.printStackTrace();
			System.out.println("The requested table was not found!!");
		}
		return true;
	}
	
	public boolean readTable(String tableName, String colfam) throws TableNotFoundException
	{
		System.out.println("========================"+tableName+"=======================");
		HTable table = null;
		try
		{
			table = getTableHandler(tableName);
			Scan scan = new Scan();
			scan.setCaching(DEFAULT_SCANNER_CACHING); 
			scan.addFamily(Bytes.toBytes(colfam));
			ResultScanner scanner = table.getScanner(scan);
			int counter=0;
			for(Result result = scanner.next(); (result != null); result = scanner.next()) 
			{
				if(counter==10)
				{
					counter++;
					break;
				}
			    Get get = new Get(result.getRow());
			    Result entireRow = table.get(get);
			    String key = Bytes.toString(result.getRow());
			    NavigableMap<byte[], byte[]> list = entireRow.getFamilyMap(Bytes.toBytes(colfam));
			    Set<byte[]> entry =  list.keySet();
			    for(byte[] colKey : entry)
            	{
			    		System.out.println("Key="+key+"--"+"value="+Bytes.toString(list.get(colKey)));
			    }
			    counter++;
			}
			table.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("Cannot read data from :"+tableName);
			try
			{
				if(table!=null) table.close();
			}
			catch(IOException ex)
			{
				System.out.println("some error in closing");
			}
			return false;
		}
		return true;
	}

	/**
	 * Creates read-only tables for forward and reverse mapping 
	 * 
	 * @param tblname
	 * @param colnames
	 * @param ColFams
	 * @return
	 */
	public boolean setupMapping(ArrayList<String> colnames,String tblSuffix) // FIXME -- in normal environs not required suffix
	{
		String tblFdwMap=AMBIENCE_tables.fwdMap.getName()+tblSuffix;
		String tblRevMap=AMBIENCE_tables.revMap.getName()+tblSuffix;
		byte[] fmapCf=Bytes.toBytes(AMBIENCE_tables.fwdMap.getColFams()[0]);
		byte[] rmapCf=Bytes.toBytes(AMBIENCE_tables.revMap.getColFams()[0]);
		byte[] qual=Bytes.toBytes(Constants.mapTblQual);
		if(!createTable(tblFdwMap,AMBIENCE_tables.fwdMap.getColFams())) return false;
		if(!createTable(tblRevMap,AMBIENCE_tables.revMap.getColFams())) return false;
		HTable fmap=null,rmap=null;
		try
		{
			try
			{
				fmap=getTableHandler(tblFdwMap);
				rmap=getTableHandler(tblRevMap);
			}
			catch(TableNotFoundException tnex) // should not happen -- but who knows
			{
				fmap=new HTable(conf,tblFdwMap);
				rmap=new HTable(conf,tblRevMap);
			}
			Put fput=null,rput=null;
			String id,col;
			for(int i=0;i<colnames.size()-1;i++) // must not add trait as well !!
			{
				id=Integer.toString(i);
				col=colnames.get(i);
				fput=new Put(Bytes.toBytes(col));
				fput.add(fmapCf,qual,Bytes.toBytes(id));
				rput=new Put(Bytes.toBytes(id));
				rput.add(rmapCf,qual,Bytes.toBytes(col));
				fmap.put(fput);rmap.put(rput);
			}
			tables.put(tblFdwMap,fmap);
			tables.put(tblRevMap,rmap);
			/*fmap.getTableDescriptor().setReadOnly(true);
			rmap.getTableDescriptor().setReadOnly(true);*/
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
			System.out.println("MAPPING TABLES FAILURE!");
			return false;
		}
		return true;
	}
	
	/**
	 * Load data into HBase table as specified in the arguments
	 */
	public boolean loadData(String tbl,ArrayList<String> colnames,
			ArrayList<ArrayList<String>> rows,String colFams[]) throws TableNotFoundException
	{
		try
		{
			NavigableMap<HRegionInfo,ServerName> regions=getRegions(tbl);
			int splitCnt=regions.size();
			if(splitCnt > 1)
			{
				Iterator<String> keysIterator = getRegStartKeys(getRegions(tbl)).iterator();
				if(!keysIterator.hasNext()) return false;
				if(!loadPolyLith(tbl,colnames,rows,colFams,splitCnt,keysIterator)) return false;
			}
			else
			{
				if(!loadMonoLith(tbl,colnames,rows,colFams)) return false;
			}
		}
		catch(IOException iex)
		{
			System.out.println("Problems in loading data to "+tbl);
			iex.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	private boolean loadMonoLith(String tbl,ArrayList<String> colnames,
			ArrayList<ArrayList<String>> rows,String[] colFams) throws IOException, TableNotFoundException
	{
		HTable table=getTableHandler(tbl);
		int colSize = colnames.size()-1;
		byte[] indVars = Bytes.toBytes(colFams[0]);
		byte[] targetVar=Bytes.toBytes(colFams[1]);
		for(int i=0;i<rows.size();i++)
		{
			ArrayList<String> currentRow =  rows.get(i);
			Put objput = new Put(Bytes.toBytes(i));
			/**
			 * VERY IMPORTANT - NOTE
			 * Bytes.toBytes(j) -- to maintain column order sanity
			 */
			for(int j=0;j<colSize;j++)
				objput.add(indVars, Bytes.toBytes(j), Bytes.toBytes(currentRow.get(j)));
			objput.add(targetVar, Bytes.toBytes(colnames.get(colSize)), Bytes.toBytes(currentRow.get(colSize)));
			table.put(objput);
		}
		return true;
	}
	
	private boolean loadPolyLith(String tbl,ArrayList<String> colnames,ArrayList<ArrayList<String>> rows,String colFams[],
			int splitCnt,Iterator<String> keysIt) throws IOException,TableNotFoundException
	{
		
		HTable table = getTableHandler(tbl);
		int colSize = colnames.size()-1;
		int rowCnt=rows.size();
		int chunkSize=(int)Math.floor((double)rowCnt/splitCnt),chunkMod=rowCnt%splitCnt;
		boolean moduloPresent=true;
		if(chunkMod!=0) chunkSize++;
		else
			moduloPresent=false;
		byte[] indVars = Bytes.toBytes(colFams[0]);
		byte[] targetVar=Bytes.toBytes(colFams[1]);
		int suffix=1;
		String prefix=keysIt.next();
		String rowKey="";
		for(int i=0;i<rows.size();i++)
		{
			ArrayList<String> currentRow =  rows.get(i);
			rowKey=prefix+suffix;
			Put objput = new Put(Bytes.toBytes(rowKey));
			/**
			 * VERY IMPORTANT - NOTE
			 * Bytes.toBytes(j) -- to maintain column order sanity
			 */
			for(int j=0;j<colSize;j++)
				objput.add(indVars, Bytes.toBytes(j), Bytes.toBytes(currentRow.get(j))); 
			objput.add(targetVar, Bytes.toBytes(colnames.get(colSize)), Bytes.toBytes(currentRow.get(colSize)));
			table.put(objput);
			suffix++;
			if(suffix > chunkSize && keysIt.hasNext() )
			{
				System.out.println("for prefix ="+prefix+" rec cnt="+(suffix-1));
				if(moduloPresent) // when #splits unevenly divides #row 
				{
					chunkMod--;
					if(chunkMod <= 0)
						chunkSize = idempotent(chunkSize);
				}
				prefix= keysIt.next();
				suffix=1;
			}
		}
		System.out.println("for prefix ="+prefix+" rec cnt="+(suffix-1));
		return true;
	}
	
	
	
	/**
	 * 
	 * @param s
	 * @return
	 */
	private int idempotent(int s)
	{
		if(!idem)
		{
			idem=true;
			return (s-1);
		}
		return s;
	}
	
	
	/**
	 * 
	 * @param tableName
	 * @param RowKey
	 * @return
	 */
	public HashMap<String, Result> getManyRecords(String tableName,ArrayList<String> RowKeys) throws IOException,TableNotFoundException
	{
		Result rs;
		HashMap<String,Result> all=new HashMap<String,Result>();
		try
		{
			for(String rowKey:RowKeys)
			{
				HTable table = getTableHandler(tableName);
		        Get get = new Get(Bytes.toBytes(rowKey));
		        rs = table.get(get);
		        if(rs==null || rs.isEmpty())
		        {
		        	System.out.println("No element present with key "+rowKey);
		        	continue;
		        }
		        all.put(rowKey,rs);
		    }
	    }
		catch(IOException ioex)
		{
			System.out.println("Some IO exception while handling fetching of rows");
			throw ioex;
		}
		return all;
	}
	/**
	 * 
	 * @param tableName
	 * @param RowKey
	 * @return
	 */
	public Result getRecord(String tableName,String RowKey) throws IOException,ElementNotFoundException,TableNotFoundException
	{
		Result rs;
		try
		{
			HTable table = getTableHandler(tableName);
	        Get get = new Get(Bytes.toBytes(RowKey));
	        rs = table.get(get);
	        if(rs==null || rs.isEmpty())
	        	throw new ElementNotFoundException();
	    }
		catch(IOException ioex)
		{
			System.out.println("Some IO exception while handling fetching of rows");
			throw ioex;
		}
		return rs;
	}
	
	/**
	 * 
	 * @param map
	 */
	public void displayRegionInfo(String tbl) throws IOException,TableNotFoundException
	{
		NavigableMap<HRegionInfo,ServerName> regions=getRegions(tbl);
		Set<HRegionInfo> n = regions.keySet();
		System.out.println("------------------- Region Info ------------------");
		System.out.println("# of regions = "+n.size());
		System.out.println("REGION_ID\tSERVER");
		for(HRegionInfo hr : n)
    		System.out.println(hr.getRegionId()+"\t"+regions.get(hr).getServerName());
    }
	
	/**
	 * 
	 * @param k
	 * @param order
	 * @return
	 * @throws IOException
	 */
	public double orderedRep(byte[] k,Order order) throws IOException
	{
		ImmutableBytesWritable buffer = new ImmutableBytesWritable();
	    DoubleWritableRowKey d = new DoubleWritableRowKey();
	    d.setOrder(order);
	    buffer.set(k, 0, k.length);
	    return ((DoubleWritable)d.deserialize(buffer)).get();
	}
		
	/**
	 * 
	 * @return
	 */
	public boolean exit() 
	{
		boolean retVal=true;
		for(Map.Entry<String, HTable> entry : tables.entrySet())// closing all tables
		{
			try
			{entry.getValue().close();}
			catch(IOException ioex)
			{
				System.out.println("Some problem closing out "+entry.getKey());
				retVal=false;
				ioex.printStackTrace();
			}
		}
		try{hAdmin.close();}
		catch(IOException ioex){ioex.printStackTrace();retVal=false;}
		
	    return retVal;
	}
	
	
	/**
   	 * 
   	 * @param mapped
   	 * @return
   	 */
   	private void sort(String[] ele) throws NumberFormatException
   	{
   		int length=ele.length;
   		int[] sorted=new int[ele.length];
   		for(int i=0;i<length;i++)
   			sorted[i]=Integer.valueOf(ele[i]);
   		sort(sorted,0,length-1);
   		for(int i=0;i<ele.length;i++)
   				ele[i]=Integer.toString(sorted[i]);
   	}
   	
   	/**
   	 * 
   	 * @param vals
   	 * @return
   	 */
   	private void sort(int[] array,int lowerIndex, int higherIndex)
   	{
   		int i = lowerIndex;
        int j = higherIndex;
        int pivot = array[lowerIndex+(higherIndex-lowerIndex)/2];
        while (i <= j) 
        {
            while (array[i] < pivot) 
                i++;
            while (array[j] > pivot) 
                j--;
            if (i <= j)
            {
            	int temp = array[i];
                array[i] = array[j];
                array[j] = temp;
                i++;
                j--;
            }
        }
        if (lowerIndex < j)
            	sort(array,lowerIndex, j);
        if (i < higherIndex)
            	sort(array,i, higherIndex);
   	}
   	
	@Override
	public boolean add() 
	{
		return false;
	}

	@Override
	public boolean delete() 
	{
		return false;
	}

	@Override
	public boolean modify() 
	{
		return false;
	}

	@Override
	public boolean scan() 
	{
		return false;
	}
	public void tstMapping() throws IOException, NumberFormatException
	{
		try
		{
			ArrayList<String> tst=new ArrayList<String>();
			tst.add("15|0|1");
			tst.add("15|1|0");
			tst.add("1|15|0");
			tst.add("1|0|15");
			tst.add("0|1|15");
			tst.add("0|15|1");
			ArrayList<String> str;
			str=getVar(tst, "\\|");
			for(String s: str)
			{
				System.out.println("we are getting "+s);
			}
	
			// 098196|098227|099003
			tst.clear();
			tst.add("099003|098196|098227");
			tst.add("099003|098227|098196");
			tst.add("098227|099003|098196");
			tst.add("098227|098196|099003");
			tst.add("098196|098227|099003");
			tst.add("098196|099003|098227");
			str = getID(tst,"\\|");
			for(String s: str)
			{
				System.out.println("we are getting "+s);
			}
		}
		catch(ElementNotFoundException exe)
		{
			System.out.println("Element not Found");
		}
	}
		
}





