package edu.buffalo.cse.ambience.database;

public class TableNotFoundException extends Exception
{
	public TableNotFoundException()
	{
		
	}
	public TableNotFoundException (String message) 
	{
        super (message);
    }

    public TableNotFoundException (Throwable cause) 
    {
        super (cause);
    }

    public TableNotFoundException (String message, Throwable cause) 
    {
        super (message, cause);
    }
}
