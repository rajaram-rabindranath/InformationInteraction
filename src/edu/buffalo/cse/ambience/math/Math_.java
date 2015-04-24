package edu.buffalo.cse.ambience.math;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.io.IntWritable;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;

public class Math_ 
{
	/**
	 * 
	 * @param a
	 * @return
	 */
	private static double log2(double a) 
	{
		return Math.log(a) / Math.log(2);
	}
	  
	/**
	 * computes continuous entropy
	 * @return
	 */
	public static double entropyCont_norm(double[][] mat,int cnt)
	{
		double entropy=0.0f;
		/*int rows=mat.length;
		cern.jet.math.Functions F = cern.jet.math.Functions.functions;
		DenseDoubleMatrix2D DM = new DenseDoubleMatrix2D(mat);
        DoubleMatrix2D covM = Statistic.covariance(DM);
        
        covM = covM.assign(F.mult((double)rows/(rows-1)));
        Algebra A = new Algebra();
        double determinant = A.det(covM);
        entropy = Math.log(Math.pow(2*Math.PI*Math.exp(1),((double)1)/2.0)*Math.sqrt(Math.abs(determinant)));
        if(Double.isNaN(entropy) || entropy==Double.NEGATIVE_INFINITY || entropy==Double.POSITIVE_INFINITY)
        {
            System.out.println("Warning:covariance matrix is singular,assuming uniform distribution");
            double p = ((double)rows)/cnt; // reverting to simpler way of computin
            entropy = -p*Math.log(p);
        }*/
		return entropy;
	}
	
	/**
	 * At this point we need this only for -------
	 * 
	 * @param bag
	 * @param rows
	 * @param cols
	 * @return
	 */
	public static double[][] makeMatrix(HashBag bag,int rowCnt,int colCnt)
	{
		double[][] mat=new double[rowCnt][colCnt];
		Set<String> vals = bag.uniqueSet();
		int rowIndex=0;
		int cnt=0;
		for(String val:vals)
		{	
			cnt=bag.getCount(val);
			for(int i=rowIndex;i<rowIndex+cnt;i++)
				mat[i][0]=Double.valueOf(val);
			rowIndex+=cnt;
		}
		return mat;
	}
	
	/**
	 * computes mixed entropy
	 * @return
	 */
	public static double entropyMixed_norm(HashMap<String,HashBag> mixedBag,int totCnt)
	{
	    double a=0,b=0,e=0,p=0,ent=0,entDiscrete=0,entCont=0;;
        /*double mat[][];
        HashBag bag;int setRowCnt;
        Set<String> keys = mixedBag.keySet();
        for(String k:keys)
        {
        	bag=mixedBag.get(k);
            setRowCnt=bag.size();
            p = (double)setRowCnt/(double)totCnt;
            mat=makeMatrix(bag,bag.size(),1);
            e = Math_.entropyCont_norm(mat,totCnt);
            a = p*Math.log(p);
            b = p*e;
            entDiscrete-= a; //H(discrete).
            entCont+= b; //H(continuous|discrete).
        }
        ent = entDiscrete + entCont;*/
        return ent;
	}
	
	/**
	 * @param bunch
	 * @param count
	 * @return
	 */
	public static double entropy(HashMap<String,Integer> bunch,double count)
	{
		double e = 0.0f;
		for (Map.Entry<String, Integer> entry : bunch.entrySet()) 
	    {
		  double p = (double) entry.getValue() /(double) count;
	      e += p*(Math.log(p)/Math.log(2));
	    }
	    return -e;
	}
	
	/**
	 * 
	 * @param bunch
	 * @param count
	 * @return
	 */
	public static double entropy(HashBag bunch,double count)
	{
		double e = 0.0f;
		Set<String> keys = bunch.uniqueSet();
		for(String key:keys) 
	    {
		  double p = (double) bunch.getCount(key) /(double) count;
	      e += p*(Math.log(p)/Math.log(2));
	    }
		return -e;
	}
	
	
	/**
	 * 
	 * @return
	 */
	public static double entropyContinuous()
	{
		double entropyContinuous=0.0f;
		return entropyContinuous;
	}
	
	/**
	 * 
	 * @return
	 */
	public static double entropyMixed()
	{
		double result=0.0f;
		return result;
	}

	/**
	 * 
	 * @return
	 */
	public static double mean()
	{
		double result=0.0f;
		return result;
	}
	
	
	/**
	 * 
	 * @param values
	 * @param mean
	 * @param count
	 * @return
	 */
	public static double variance(Iterable<IntWritable> values,double mean, double count)
	{
		double result=0.0f;
		//for()
		return result;
	}
}
