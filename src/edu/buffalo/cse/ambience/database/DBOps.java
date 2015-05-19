package edu.buffalo.cse.ambience.database;

import edu.buffalo.cse.ambience.core.AMBIENCE_tables;
import edu.buffalo.cse.ambience.dataStructures.Table;


public interface DBOps 
{
	public boolean add();
	public boolean delete();
	public boolean modify();
	public boolean scan();
	public boolean createTable(String tableName, String[] cols);
	//public boolean loadData(AMBIENCE_tables tbl, Table data); -- FIXME
}
