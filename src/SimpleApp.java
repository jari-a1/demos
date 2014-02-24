
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
   // test1();
	  test2();
  }
  
  public static void  test1(){
	  String logFile = "/Users/jari/a1dev/idea/software/spark/README.md"; // Should be some file on your system
	    JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
	      "/Users/jari/a1dev/idea/software/spark", new String[]{"/Users/jari/a1dev/idea/idea1/target/simple-project-1.0.jar"});
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();
	    
	    long nums = logData.filter(new Function<String, Boolean>() {
	        public Boolean call(String s) { return s.contains("b"); }
	      }).count();
	    
	    long counts = logData.count();
	  

	    System.out.println("Total Lines : "+counts+" Lines with a: " + numAs + ", lines with b: " + numBs);

  }
  
  public static void  test2(){
	  String dataFile1 = "/Users/jari/a1dev/idea/idea1/data/LifeGDP.txt"; // Should be some file on your system
	    JavaSparkContext sc1 = new JavaSparkContext("local", "Simple App1",
	      "/Users/jari/a1dev/idea/idea1/LifeGDP.txt", new String[]{"/Users/jari/a1dev/idea/idea1/target/simple-project-1.0.jar"});
	    JavaRDD<String> logData1 = sc1.textFile(dataFile1).cache();
	    
	    String dataFile2 = "/Users/jari/a1dev/idea/idea1/data/FLifeExpTable.txt"; // Should be some file on your system
	    JavaRDD<String> logData2 = sc1.textFile(dataFile2).cache();
	   

	    long numAs = logData1.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData1.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();
	    
	    long nums = logData1.filter(new Function<String, Boolean>() {
	        public Boolean call(String s) { return s.contains("b"); }
	      }).count();
	    
	    JavaRDD dataset =logData1.filter(new Function<String, Boolean>() {
	        public Boolean call(String s) { return s.contains("c"); }
	      });
	    
	    JavaRDD dataset2 =logData1.filter(new Function<String, Boolean>() {
	        public Boolean call(String s) { return s.contains("c"); }
	      });
	    
	  //  JavaPairRDD dataset3 =dataset.map();
	   // JavaPairRDD dataset4 =dataset3.join(dataset2);
	    
	    long counts = logData1.count();
	    String first = (String) logData1.first();

	    System.out.println("Total Lines : "+counts+" Lines with a: " + numAs + ", lines with b: " + numBs);
	    System.out.println("First:"+first);
	    System.out.println("Lines with c: "+dataset2.count());
	    String first2 = (String) logData2.first();
	    System.out.println("First:"+first2);
  }
  
}
