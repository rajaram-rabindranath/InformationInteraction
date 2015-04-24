package edu.buffalo.cse.ambience.dataStructures;


import java.util.ArrayList;
import java.util.HashMap;

/**
 * Just a package to get the data back to the caller of 
 * the function readInput -- beyond this there is no purpose
 * to this class
 * @author dev
 */
public class Table
{
	private Columns colnames;
	private Rows rows;
		
	private static Table instance;
	
	private VarClass[] type; 
	private HashMap<Integer,String> VAR_MAPn;
	private HashMap<String,Integer> REV_MAPn;
	private HashMap<Integer,String> VAR_MAP;
	private HashMap<String,Integer> REV_MAP;
	
	private int colCount;
	private int rowCount;
	
	
	public static Table getInstance()
	{
		return instance;
	}
	
	public static Table getInstance(Columns colnames,Rows rows)
	{
		if(instance == null)
		{
			instance =  new Table(colnames, rows);
		}
		return instance;
	}
	
	private Table(Columns colnames,Rows rows)
	{
		this.colnames= colnames;
		this.rows= rows;
		this.colCount = colnames.size();
		this.rowCount = rows.size();
		int k =0;
		type=new VarClass[colCount];
		setTypes();
		VAR_MAP = new HashMap<Integer,String>();
		REV_MAP = new HashMap<String,Integer>();
		for(String colName :colnames.c)
		{
			VAR_MAP.put(k,colName);
			REV_MAP.put(colName,k);
			k++;
		}
	}
	
	/**
	 * Compress -- does an id to value mapping --
	 * where the id is a smaller takes up less space
	 * @param combo
	 */
	public String getMapping(String comboKey)
	{
		StringBuilder sbKey=new StringBuilder();
		String[] s = comboKey.split(Constants.COMB_SPLIT);
		
		HashMap<Integer,String> dictionary= VAR_MAPn != null ? VAR_MAPn : VAR_MAP;
		for(int index=0;index<s.length;index++)
		{
			sbKey.append(dictionary.get(Integer.valueOf(s[index])));
			sbKey.append(Constants.COMB_SEP);
		}
		sbKey.deleteCharAt(sbKey.length()-1);
		return sbKey.toString();
	}
	
	
	/**
	 * default -- FIXME -- must take 
	 * input from the user
	 */
	public void setTypes()
	{
		for(int i=0;i<type.length-1;i++)
			type[i]=VarClass.Discrete;
		type[type.length-1]=VarClass.Continuous;
	}
	
	/**
	 * 
	 * @param cmb
	 * @return
	 */
	public VarClass[] getTypes(String cmb) // FIXME -- must be static how to do this
	{
		String[] cols=cmb.split(Constants.COMB_SPLIT);
		VarClass[] t=new VarClass[cols.length];
		for(int i=0;i<t.length;i++)
			t[i]=type[Integer.valueOf(cols[i])];
		return t;
	}
	
	
	/**
	 * 
	 * @param colnames
	 */
	public void setDictionary(ArrayList<String> colnames)
	{
		int k =0;
		VAR_MAPn = new HashMap<Integer,String>();
		REV_MAPn = new HashMap<String,Integer>();
		for(String colName :colnames)
		{
			VAR_MAPn.put(k,colName);
			REV_MAPn.put(colName,k);
			k++;
		}
	}
	
	public Rows getRows()
	{
		return rows;
	}
	
	public Columns getColumns()
	{
		return colnames;
	}
	
	public int getColCount()
	{
		return colCount;
	}
	
	public int getMRColsCnt()
	{
		return colCount-1;
	}
	
	public int getRowCount()
	{
		return rowCount;
	}
	
	public String getColName(int id)
	{
		return VAR_MAP.get(id);
	}
	
	public HashMap<Integer,String> getVarMap()
	{
		return VAR_MAP;
	}
	
	public int getColID(String name)
	{
		return REV_MAP.get(name);
	}
	
	public String getTargerVar()
	{
		return colnames.getTargetVar();
	}
}