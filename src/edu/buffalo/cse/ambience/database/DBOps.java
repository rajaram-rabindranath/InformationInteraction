package edu.buffalo.cse.ambience.database;


public interface DBOps 
{
	public boolean add();
	public boolean delete();
	public boolean modify();
	public boolean scan();
	public void tableDump(String tbl,String dumpName,String colfam,String qual);
}
