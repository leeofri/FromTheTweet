package solution;import org.apache.hadoop.fs.Path;

public class Globals {
	static int kmeansCount = 0 ;
	static int daysNumber = 0 ;
	static int featuresNumber  =  0;
	static boolean LastReduce = false;
	static String outputFolder = "./output";
	
	public static double T1() {
		return 29.95;
	}
	
	public static double T2() {
		return 0;
	}
	
	public static Path CanopyCenterPath()
	{
		return new Path("./data/SequenceFile.canopyCenters");
	}
	
//	public static Path InputFolder()
//	{
//		return new Path("./input/input");
//	}
	
	public static Path getOutputFolder()
	{
		return new Path(outputFolder);
	}
	
	public static void setOutputFolder(String outputPath)
	{
		outputFolder = outputPath;
	}
	
	public static Path OutputFolderCanopy()
	{
		return new Path(Globals.getOutputFolder()+"/Canopy");
	}
	
	public static Path OutputFolderKmeans(int iteration)
	{
		return new Path(Globals.getOutputFolder()+"/Kmeans"+iteration);
	}
	
	public static Path KmeansCenterPath()
	{
		return new Path("./data/SequenceFile.kmeansCenters");
	}
	
	public static Path UserConfigFilePath()
	{
		return new Path("./data/userConfigFile.config");
	}
	
	public static int getKmeansCount()
	{
		return kmeansCount;
	}
	
	public static void setKmeansCount(int count)
	{
		 kmeansCount = count;
	}
	
	public static int getDaysNumber()
	{
		return daysNumber;
	}
	
	public static void setDaysNumber(int count)
	{
		daysNumber = count;
	}
	
	
	public static int getFeaturesNumber()
	{
		return featuresNumber ;
	}
	
	public static void setFeaturesNumber(int count)
	{
		featuresNumber  = count;
	}
	public static boolean isLastReduce()
	{
		return LastReduce;
	}
	public static double getKmeansZeroDistance()
	{
		return 0.8;
	}
	public static void turnOnLastRunFlag()
	{
		LastReduce = true;
	}
	
}
