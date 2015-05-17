package edu.buffalo.cse.ambience.database;

public class ElementNotFoundException extends Exception 
{
	public ElementNotFoundException()
	{
		
	}
	public ElementNotFoundException (String message) 
	{
        super (message);
    }

    public ElementNotFoundException (Throwable cause) 
    {
        super (cause);
    }

    public ElementNotFoundException (String message, Throwable cause) 
    {
        super (message, cause);
    }
	
}
