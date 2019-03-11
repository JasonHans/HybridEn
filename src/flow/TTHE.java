package flow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.DataInputStream;

import org.apache.commons.configuration.*;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapreduce.InputSplit; 
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import p3.hadoop.mapreduce.lib.input.PcapInputFormat;

public class TTHE{
	
	public static class Mapper01 extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
				
	    //out：<"srcIp"+flowDirection+" " + n + " " + srcIp + " " + protocol + " " + ctrlBit, 1>
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
								
			double interval = 10;//the default interval is 10s					
			int tStart=0;
			
			//get parameter
			String s = context.getConfiguration().get("interval");
			interval = Double.parseDouble(s)*1000000;//时间单位统一到微秒
            String line = value.toString();
                       
            if(!line.substring(0,1).equals("#")){
                try{
                	//将一行中所有的域都取出来，然后赋值给一个数组field[]
            	    String field[] = line.split(",");
            	    
            	    if(field.length == 20){
            	    	
            	    	String srcIp = new String(field[1].trim());
            	    	String dstIp = new String(field[2].trim());
            	    	String protocol = new String(field[4].trim());
            	    	String srcPt = new String(field[5].trim());
            	    	String dstPt = new String(field[6].trim());
            	    	String srcAs = new String(field[11].trim());
            	    	String dstAs = new String(field[12].trim());
            	    	String ctrlBit = new String(field[14].trim());
            	    	
					    long tFirst = Long.parseLong(field[18].trim())*1000;//时间单位统一到微秒
            	        long tLast = Long.parseLong(field[19].trim())*1000;
					    //区分四种流量,0为校外流量，出流量为1，入流量为2，校内流量为3
            	    	int flowDirection = 0;
					    if ((!dstAs.equals(srcAs)) && srcAs.equals("45576")){
					        flowDirection = 1;
					    }
					    if ((!dstAs.equals(srcAs)) && dstAs.equals("45576")){
					        flowDirection = 2;
					    }
					    if (dstAs.equals(srcAs) && srcAs.equals("45576")){
					        flowDirection = 3;
					    }

					    //划分时间片，并按照时间片将当前流的四个域打标签
					    String ele1 = new String(srcIp + "-" + protocol + "-"+ ctrlBit);
					    String ele2 = new String(dstIp + "-" + protocol + "-"+ ctrlBit);
					    String ele3 = new String(srcPt + "-" + protocol + "-" + ctrlBit);
					    String ele4 = new String(dstPt + "-" + protocol + "-" + ctrlBit);

					    long n= (long)Math.floor((tLast - tStart) / interval);//得到取出来的流的时间片序号

					    context.write(new Text("srcIp" + flowDirection + " " + n + " " + ele1 ), one);
					    context.write(new Text("dstIp" + flowDirection + " " + n + " " + ele2 ), one);
					    context.write(new Text("srcPt" + flowDirection + " " + n + " " + ele3 ), one);
					    context.write(new Text("dstPt" + flowDirection + " " + n + " " + ele4 ), one);
							            	    
					    /*考虑这条流横跨多个时间片的情况
					   	if((long)Math.floor((tLast - tStart * 1000) / interval) > n){
						  	while(n++ <= (long)Math.floor((tLast - tStart*1000) / interval)){
						        context.write(new Text("srcIp"+flowDirection+" " + n + " " + ele1 ), one);
						        context.write(new Text("dstIp"+flowDirection+" " + n + " " + ele2 ), one);
						        context.write(new Text("srcPt"+flowDirection+" " + n + " " + ele3 ), one);
						        context.write(new Text("dstPt"+flowDirection+" " + n + " " + ele4 ), one);							  
						  }
					   }*/
            	    }
			    }catch(IOException e){
			    	e.printStackTrace(); 
	            }
		    }
		}
	}
	
    //out：<srcIp1 0 192.168.1.1 6 2,200>
    public static class Combiner01 extends Reducer<Text, IntWritable, Text, IntWritable> {
	
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
		 InterruptedException {
	    	
			 int sum = 0;
		        for (IntWritable val : values) {//将相同Key的value相加
		            sum += val.get();
		        }
		        context.write(key, new IntWritable(sum));
		    }
		 }
    
    //in：<srcIp1 0 192.168.1.1 6 2,200> out：<srcIp1 0 192.168.1.1 6 2 201>    
    public static class Reducer01 extends Reducer<Text, IntWritable, Text, NullWritable> {
    	
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
		InterruptedException {
				
			int sum = 0;
            for (IntWritable val : values) {
	            sum += val.get();
            }		        
	        context.write(new Text(key + " " + sum),null);	        
	    }   	
    }
    //in：srcIp1 0 192.168.1.1 3000 
	public static class Mapper02 extends Mapper<LongWritable, Text, Text, Text>{
	
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        	 String line = values.toString();
             String array[] = line.split(" ");
         	if(array.length == 4){
                 context.write(new Text(array[0] + " " + array[1]),new Text(array[2] + " " + array[3]));
         	}										
 		}					
 	}
 	
 	//in：<srcIp1 0,201> ，out <srcIp1 0 entropy total>
 	public static class Reducer02 extends Reducer<Text, Text, Text,NullWritable > {

	     public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
	        double total = 0;
	        double total1 = 0;
	        double total2 = 0;
	        double entropy1 = 0;
	        double entropy2 = 0;
	        double k = Double.parseDouble(context.getConfiguration().get("k"));
	        double q = Double.parseDouble(context.getConfiguration().get("q"));
	        double a = Double.parseDouble(context.getConfiguration().get("a"));
	        double DT = Double.parseDouble(context.getConfiguration().get("DT"));
	        double z = Double.parseDouble(context.getConfiguration().get("z"));
	        double HT = Double.parseDouble(context.getConfiguration().get("HT"));
			String s3 = context.getConfiguration().get("filter");
			double filter = Double.parseDouble(s3);
			List<Double> list = new ArrayList<Double>();
			List<Double> list1 = new ArrayList<Double>();
			List<Double> list2 = new ArrayList<Double>();
			String ht = "";
			String lHt = "";
			double tval = 0;
        	for (Text val : values) {
        		String tmp = val.toString();
        		String arr[] = tmp.split(" ");
        		if(arr.length == 2){
        			tval=Double.parseDouble(arr[1]);
        			total += tval;
        		}
	            list.add(tval);
	        	//heavy hitters
	        	if(tval >= HT){
	        		ht =  new String(key + " " + arr[0] + " " + tval);
	        		lHt = new String(lHt + ":" + ht);
	        	}
        	}	       

	        if(total >= filter)
	        {
		        for (int i = 0; i < list.size(); i++) {
		        	double tmp = (double)list.get(i);
		        	if(tmp >= total * DT){
		        		total2 += tmp;
		        		list2.add(tmp);
		        	}else{
		        		total1 += tmp;
		        		list1.add(tmp);
		        	}
				}
		        //compute entro1  分了低概率和高概率进行计算，添加z个新元素，概率为  1/(z+total2)
		        //低概率Tsallis熵
		        double sum1 = 0;
		        for (int i = 0; i < list1.size(); i++) {
		        	double tmp = (double)list1.get(i);
			        double p1 = tmp / total1;	
			        sum1 += Math.pow(p1, q);
				}
		        entropy1 = k * (1 - sum1) / (q - 1);
		        
		        //compute entro2 高概率
		        double sum2 = 0;
		        for (int i = 0; i < list2.size(); i++) {
		        	double tmp = (double)list2.get(i);
			        double p1 = tmp / (total2 + z);	
			        sum2 += Math.pow(p1, a);
				}
		        double p2 = (double)1 / (total2 + z);
		        sum2 += z * Math.pow(p2, a);
	        	entropy2 += Math.log(sum2) / (1-a);
	        	
		        context.write(new Text(key + " " + entropy1 + " " + entropy2 + " " + total), null);
		        context.write(new Text(lHt), null);
	        }	
	     }
	}
 	
	public static class Mapper03 extends Mapper<LongWritable, Text, Text, LongWritable>{
		
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        	
        	context.write(values, key);
		}					
	}
	

	public static class Reducer03 extends Reducer<Text, LongWritable, Text,NullWritable > {
		 
	     public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, 
	     InterruptedException {
			
	    	 context.write(key,null);
	    	 
	     }
	}

	//文件格式转换，输入为<srcIp1 0 entropy total>，输出为<engtropy1 engtropy2 engtropy3 engtropy4 timeWin total>
	public void finallyEntroFile(String inpath, String outpath, double z)
	{
        System.out.println("finallyEntroFile "+inpath);     
			 double maxEntro = 0;
			 double maxEntro1 = 0;
             double entro=0;
             double entro1=0;
             String strFlowType;
             String line;
             String outputFile0 = new String(outpath+"/outer");  
             String outputFile1 = new String(outpath+"/out");    
             String outputFile2 = new String(outpath+"/in");   
             String outputFile3 = new String(outpath+"/inner");   
             int entroSum0 = 0;
             int entroSum1 = 0;
             int entroSum2 = 0;
             int entroSum3 = 0;                         
             int acc0_0 = 0;
             int acc0_1 = 0;
             int acc0_2 = 0;
             int acc0_3 = 0;
             int acc1_0 = 0;
             int acc1_1 = 0;
             int acc1_2 = 0;
             int acc1_3 = 0;
             int acc2_0 = 0;
             int acc2_1 = 0;
             int acc2_2 = 0;
             int acc2_3 = 0;
             int acc3_0 = 0;
             int acc3_1 = 0;
             int acc3_2 = 0;
             int acc3_3 = 0;
             //org.apache.hadoop.conf.Configuration conf = new JobConf();     
             
	         try
	        {    
	        	 FileSystem fs = FileSystem.get(new JobConf());	  
	        	 FSDataInputStream fin1 = fs.open(new Path(inpath));
	        	 BufferedReader bufReader1 = null;
	        	 bufReader1 = new BufferedReader(new InputStreamReader(fin1));
	        	 //line = bufReader1.readLine();
	        	 //System.out.println("line:"+line);
                 //compute the max line number
                 while((line = bufReader1.readLine()) != null){
                	String array[] = line.split(" ");

                 		if(array[0].equalsIgnoreCase("dstIp0")){
                		   entroSum0++;
                	    }
                	    if(array[0].equalsIgnoreCase("dstIp1")){
                		   entroSum1++;
                	    }
                	    if(array[0].equalsIgnoreCase("dstIp2")){
                		   entroSum2++;
                	    } 
                	    if(array[0].equalsIgnoreCase("dstIp3")){
                		   entroSum3++;
                	    }
                	    if(array[0].substring(0,5).equalsIgnoreCase("dstPt")){
                		   break;
                	    } 
                 }
                 //bufReader1.close();
                 //fin1.close();
                 //System.out.println("entroSum0:"+entroSum0+"|entroSum1:"+entroSum1+"|entroSum2:"+entroSum2+
                 //"|entroSum3:"+entroSum3);                 
                 //format:srcIp dstIp srcPt dstPt
                 double field0[][] = new double[entroSum0][4];//srcipEntro dstipEntro srcptEntro dstptEntro timeWin totalFlow
                 double field1[][] = new double[entroSum1][4];
                 double field2[][] = new double[entroSum2][4];
                 double field3[][] = new double[entroSum3][4];
                 String time0[] = new String[entroSum0];
                 String time1[] = new String[entroSum1];
                 String time2[] = new String[entroSum2];
                 String time3[] = new String[entroSum3];
                 double total0[] = new double[entroSum0];
                 double total1[] = new double[entroSum1];
                 double total2[] = new double[entroSum2];
                 double total3[] = new double[entroSum3];
                 double field4[][] = new double[entroSum0][4];//srcipEntro dstipEntro srcptEntro dstptEntro timeWin totalFlow
                 double field5[][] = new double[entroSum1][4];
                 double field6[][] = new double[entroSum2][4];
                 double field7[][] = new double[entroSum3][4];
                 //read and write
	        	 FSDataInputStream fin = fs.open(new Path(inpath));
	        	 BufferedReader bufReader = null;
	        	 bufReader = new BufferedReader(new InputStreamReader(fin));                                

	             while ((line= bufReader.readLine())!= null)
	             {
                     //<srcIp1 0 entropy total maxEntropy>
	            	 String array[] = line.split(" ");                     
                     strFlowType = array[0];//求出所属的域类型
                     String timeWin = array[1];
                     double totalSum = Double.parseDouble(array[4]);
                     entro = Double.parseDouble(array[2]);//low
                     entro1 = Double.parseDouble(array[3]);//high
                     //System.out.println("entro: "+entro+" entro1:"+entro1);
                     //type0:校外
	                 if(strFlowType.equals("srcIp0")) {
	                	 field0[acc0_0][0] = entro;
	                	 field4[acc0_0][0] = entro1;
	                	 time0[acc0_0] = timeWin; 
	                	 total0[acc0_0] = totalSum;
	                	 acc0_0++;
	                 }
	                 if(strFlowType.equals("dstIp0")) {
	                	 field0[acc0_1][1] = entro; 
	                	 field4[acc0_1][1] = entro1;
	                	 acc0_1++;
	                 }
	                 if(strFlowType.equals("srcPt0")) {
	                	 field0[acc0_2][2] = entro;
	                	 field4[acc0_2][2] = entro1; 
	                	 acc0_2++;
	                 }
	                 if(strFlowType.equals("dstPt0")) {
	                	 field0[acc0_3][3] = entro;
	                	 field4[acc0_3][3] = entro1;
	                	 acc0_3++;
	                 } 
                     //type1:出校
	                 if(strFlowType.equals("srcIp1")) {
	                	 field1[acc1_0][0] = entro; 
	                	 field5[acc1_0][0] = entro1;
	                	 time1[acc1_0] = timeWin; 
	                	 total1[acc1_0] = totalSum;
	                	 acc1_0++;
	                 }
	                 if(strFlowType.equals("dstIp1")) {
	                	 field1[acc1_1][1] = entro;
	                	 field5[acc1_1][1] = entro1; 
	                	 acc1_1++;
	                 }
	                 if(strFlowType.equals("srcPt1")) {
	                	 field1[acc1_2][2] = entro; 
	                	 field5[acc1_2][2] = entro1;
	                	 acc1_2++;
	                 }
	                 if(strFlowType.equals("dstPt1")) {
	                	 field1[acc1_3][3] = entro;
	                	 field5[acc1_3][3] = entro1;
	                	 acc1_3++;
	                 } 
                     //type2:入校
	                 if(strFlowType.equals("srcIp2")) {
	                	 field2[acc2_0][0] = entro; 
	                	 field6[acc2_0][0] = entro1; 
	                	 time2[acc2_0] = timeWin; 
	                	 total2[acc2_0] = totalSum;
	                	 acc2_0++;
	                 }
	                 if(strFlowType.equals("dstIp2")) {
	                	 field2[acc2_1][1] = entro; 
	                	 field6[acc2_1][1] = entro1; 
	                	 acc2_1++;
	                 }
	                 if(strFlowType.equals("srcPt2")) {
	                	 field2[acc2_2][2] = entro; 
	                	 field6[acc2_2][2] = entro1; 
	                	 acc2_2++;
	                 }
	                 if(strFlowType.equals("dstPt2")) {
	                	 field2[acc2_3][3] = entro; 
	                	 field6[acc2_3][3] = entro1;
	                	 acc2_3++;
	                 } 
                     //type3:校内
	                 if(strFlowType.equals("srcIp3")) {
	                	 field3[acc3_0][0] = entro; 
	                	 field7[acc3_0][0] = entro1;
	                	 time3[acc3_0] = timeWin;
	                	 total3[acc3_0] = totalSum;
	                	 acc3_0++;
	                 }
	                 if(strFlowType.equals("dstIp3")) {
	                	 field3[acc3_1][1] = entro; 
	                	 field7[acc3_1][1] = entro1; 
	                	 acc3_1++;
	                 }
	                 if(strFlowType.equals("srcPt3")) {
	                	 field3[acc3_2][2] = entro; 
	                	 field7[acc3_2][2] = entro1;
	                	 acc3_2++;
	                 }
	                 if(strFlowType.equals("dstPt3")) {
	                	 field3[acc3_3][3] = entro; 
	                	 field7[acc3_3][3] = entro1;
	                	 acc3_3++;
	                 } 
	                 //for normalization
	                 if(entro > maxEntro){
	                	 maxEntro = entro;
	                 }
	                 if(entro1 > maxEntro1){
	                	 maxEntro1 = entro1;
	                 }
				 }

	             bufReader.close();
	             fin.close();
	             
	             //for normalization
		        	//maxEntro = -Math.log((double)1/z);	
		        	
	             //write file
	             String str; 
	             //FileSystem hdfs = FileSystem.get(conf);	         	
		            
		            if (fs.exists(new Path(outputFile0))){  
		            	fs.delete(new Path(outputFile0),true);
		            	System.out.println("write "+outputFile0);
		            } 
		            if (fs.exists(new Path(outputFile1))){  
		            	fs.delete(new Path(outputFile1),true);
		            	System.out.println("write "+outputFile1);
		            } 
		            if (fs.exists(new Path(outputFile2))){  
		            	fs.delete(new Path(outputFile2),true);
		            	System.out.println("write "+outputFile2); 
		            } 
		            if (fs.exists(new Path(outputFile3))){  
		            	fs.delete(new Path(outputFile3),true);
		            	System.out.println("write "+outputFile3);
		            } 		            
		            //write outer.txt
			        FSDataOutputStream fout = fs.create(new Path(outputFile0));
			        BufferedWriter bufferWriter = null;
			        bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));
		    	    for(int j=0;j<entroSum0-1;j++) 
		 		    { 
		    	    	String lEn = new String(field0[j][0]/maxEntro+" "+field0[j][1]/maxEntro+" "+
		    	    			field0[j][2]/maxEntro+" "+field0[j][3]/maxEntro);
		    	    	String hEn = new String(field4[j][0]/maxEntro1+" "+field4[j][1]/maxEntro1+" "+
		    	    			field4[j][2]/maxEntro1+" "+field4[j][3]/maxEntro1);
		    	        str = new String(lEn+" "+hEn+" "+time0[j]+" "+total0[j]);
		    	        bufferWriter.write(str);
		    	        bufferWriter.flush();
		    	        bufferWriter.newLine();
		 		    }
		    	    if(entroSum0 > 1){
		    	    	String lEn = new String(field0[entroSum0-1][0]/maxEntro+" "+field0[entroSum0-1][1]/maxEntro+" "+
		    	    			field0[entroSum0-1][2]/maxEntro+" "+field0[entroSum0-1][3]/maxEntro);
		    	    	String hEn = new String(field4[entroSum0-1][0]/maxEntro1+" "+field4[entroSum0-1][1]/maxEntro1+" "+
		    	    			field4[entroSum0-1][2]/maxEntro1+" "+field4[entroSum0-1][3]/maxEntro1);
		    	    	str = new String(lEn+" "+hEn+" "+time0[entroSum0-1]+" "+total0[entroSum0-1]);
		    	        bufferWriter.write(str);
		    	        bufferWriter.flush();
		    	    }		            
	    	        //write out.txt
	    	        fout = fs.create(new Path(outputFile1));
	    	        bufferWriter = null;
			        bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));
		    	    for(int j=0;j<entroSum1-1;j++) 
		 		    { 
		    	    	String lEn = new String(field1[j][0]/maxEntro+" "+field1[j][1]/maxEntro+" "+field1[j][2]/maxEntro+
		    	    			" "+field1[j][3]/maxEntro);
		    	    	String hEn = new String(field5[j][0]/maxEntro1+" "+field5[j][1]/maxEntro1+" "+field5[j][2]/maxEntro1+
		    	    			" "+field5[j][3]/maxEntro1);
		    	        str = new String(lEn+" "+hEn+" "+time1[j]+" "+total1[j]);
		    	        bufferWriter.write(str);
		    	        bufferWriter.flush();
		    	        bufferWriter.newLine();
		 		    }
		    	    if(entroSum1 > 1){
		    	    	String lEn = new String(field1[entroSum1-1][0]/maxEntro+" "+field1[entroSum1-1][1]/maxEntro+" "+
		    	    			field1[entroSum1-1][2]/maxEntro+" "+field1[entroSum1-1][3]/maxEntro);
		    	    	String hEn = new String(field5[entroSum1-1][0]/maxEntro1+" "+field5[entroSum1-1][1]/maxEntro1+" "+
		    	    			field5[entroSum1-1][2]/maxEntro1+" "+field5[entroSum1-1][3]/maxEntro1);
		    	    	str = new String(lEn+" "+hEn+" "+time1[entroSum1-1]+" "+total1[entroSum1-1]);
		    	    	bufferWriter.write(str);
		    	        bufferWriter.flush(); 
		    	    }      
	    	        //write in.txt
	    	        fout = fs.create(new Path(outputFile2));
	    	        bufferWriter = null;
			        bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));
		    	    for(int j=0;j<entroSum2-1;j++) 
		 		    { 
		    	    	String lEn = new String(field2[j][0]/maxEntro+" "+field2[j][1]/maxEntro+" "+field2[j][2]/maxEntro+
		    	    			" "+field2[j][3]/maxEntro);
		    	    	String hEn = new String(field6[j][0]/maxEntro1+" "+field6[j][1]/maxEntro1+" "+field6[j][2]/maxEntro1+
		    	    			" "+field6[j][3]/maxEntro1);
		    	        str = new String(lEn+" "+hEn+" "+time2[j]+" "+total2[j]);
		    	        bufferWriter.write(str);
		    	        bufferWriter.flush();
		    	        bufferWriter.newLine();
		 		    }
	    	        if(entroSum2 > 1){
	    	        	String lEn = new String(field2[entroSum2-1][0]/maxEntro+" "+field2[entroSum2-1][1]/maxEntro+" "+
	    	        			field2[entroSum2-1][2]/maxEntro+" "+field2[entroSum2-1][3]/maxEntro);
		    	    	String hEn = new String(field6[entroSum2-1][0]/maxEntro1+" "+field6[entroSum2-1][1]/maxEntro1+" "+
		    	    			field6[entroSum2-1][2]/maxEntro1+" "+field6[entroSum2-1][3]/maxEntro1);
	    	        	str = new String(lEn+" "+hEn+" "+time2[entroSum2-1]+" "+total2[entroSum2-1]);
	    	            bufferWriter.write(str);
	    	            bufferWriter.flush(); 
	    	        }
		    	    
	    	        //write inner.txt
	    	        fout = fs.create(new Path(outputFile3));
	    	        bufferWriter = null;
			        bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));
		    	    for(int j=0;j<entroSum3-1;j++) 
		 		    { 
		    	    	String lEn = new String(field3[j][0]/maxEntro+" "+field3[j][1]/maxEntro+" "+field3[j][2]/maxEntro+
		    	    			" "+field3[j][3]/maxEntro);
		    	    	String hEn = new String(field7[j][0]/maxEntro1+" "+field7[j][1]/maxEntro1+" "+field7[j][2]/maxEntro1+
		    	    			" "+field7[j][3]/maxEntro1);
		    	        str = new String(lEn+" "+hEn+" "+time3[j]+" "+total3[j]);
		    	        bufferWriter.write(str);
		    	        bufferWriter.flush();
		    	        bufferWriter.newLine();
		 		    }
		    	    if(entroSum3>0){
		    	    	String lEn = new String(field3[entroSum3-1][0]/maxEntro+" "+field3[entroSum3-1][1]/maxEntro+" "+
		    	    			field3[entroSum3-1][2]/maxEntro+" "+field3[entroSum3-1][3]/maxEntro);
		    	    	String hEn = new String(field7[entroSum3-1][0]/maxEntro1+" "+field7[entroSum3-1][1]/maxEntro1+" "+
		    	    			field7[entroSum3-1][2]/maxEntro1+" "+field7[entroSum3-1][3]/maxEntro1);
		    	    	str = new String(lEn+" "+hEn+" "+time3[entroSum3-1]+" "+total3[entroSum3-1]);		    	    
	    	            bufferWriter.flush();
		    	    }
		    	    bufferWriter.close();
		    	    fout.close();  
	         }
	         catch (IOException e)
	         {
	        	 e.printStackTrace(); 
	         }
	}

    void splitfile(String inpath,String efile,String hfile,String n){
    	 //read and write
    	//输入路径，输出熵文件，输出heavyhitter文件，n为流量方向
    	String htfile = hfile;
    	String entrofile = efile;
    	//n="4";//default,all direction
    	try{
    	    FileSystem fs = FileSystem.get(new JobConf());
   	        FSDataInputStream fin = fs.open(new Path(inpath));
   	        BufferedReader bufferReader = null;
   	        bufferReader = new BufferedReader(new InputStreamReader(fin));          	   	        
            if (fs.exists(new Path(htfile))){  
            	fs.delete(new Path(htfile),true);
            } 
            if (fs.exists(new Path(entrofile))){  
            	fs.delete(new Path(entrofile),true);
            } 
            //write
	        FSDataOutputStream fout1 = fs.create(new Path(htfile));
	        BufferedWriter bufferWriter1 = new BufferedWriter(new OutputStreamWriter(fout1,"UTF-8"));
	        FSDataOutputStream fout2 = fs.create(new Path(entrofile));
	        BufferedWriter bufferWriter2 = new BufferedWriter(new OutputStreamWriter(fout2,"UTF-8"));   	        
   	        
            String line = null;
            while ((line= bufferReader.readLine())!= null){
              if(line.length()>0){
            	  //heavy hitter
        	    if(line.substring(0,1).equals(":")){
        	    	String arr[] = line.split(":");
        	    	for(int i=1;i<arr.length ;i++){
        	    		if(n.equals("4")){
        	    			bufferWriter1.write(arr[i]);
    	    	            bufferWriter1.flush();
    	    	            bufferWriter1.newLine();
        	    		}
        	    		else if(arr[i].substring(5, 6).equals(n)){
        	    			bufferWriter1.write(arr[i]);
    	    	            bufferWriter1.flush();
    	    	            bufferWriter1.newLine();
        	    		}	    	                   	    		
        	    	}
        	    	//entropy
        	    }else{
    	            bufferWriter2.write(line);
    	            bufferWriter2.flush();
    	            bufferWriter2.newLine();
        	    }
            }
          }
            bufferWriter1.close();  
            fout1.close();
            bufferWriter2.close();
            fout2.close();
    	}catch (IOException e)
        {
       	 e.printStackTrace(); 
        }
    }
    
    
    /*--------------检测-------------
     * 
     * */
    void detect(String efile,String pfile,String output){
    	
    	double th[] = new double[8];
    	for(int i=0;i<8;i++){
    		th[i]=0.5;
    	}

    	 //read and write
    	try{
    	    FileSystem fs = FileSystem.get(new JobConf());
   	        FSDataInputStream fin1 = fs.open(new Path(efile));//这里有误
   	        FSDataInputStream fin2 = fs.open(new Path(efile));
   	        BufferedReader bufferReader1 = new BufferedReader(new InputStreamReader(fin1)); 
   	        BufferedReader bufferReader2 = new BufferedReader(new InputStreamReader(fin2));
            if (fs.exists(new Path(output))){  
            	fs.delete(new Path(output),true);
            } 
            HashMap<String,Number> has=new HashMap<String,Number>();
            String line = null;
            //读参数文件
            while ((line= bufferReader1.readLine())!= null){
            	String arr[] = line.split(" ");
            	for(int i=0;i<8;i++){
            		th[i]=Double.parseDouble(arr[i]);
            	}
            }

	        FSDataOutputStream fout = fs.create(new Path(output));
	        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));  	        
            
	        //读熵值文件并判断
            while ((line= bufferReader2.readLine())!= null){
            	String result = "none";
                if(line.length()>0){
        	    	String arr[] = line.split(" ");
        	    	for(int i=4;i<8;i++){
        	    		if(Double.parseDouble(arr[i])<th[i]){
        	    			result = arr[8];
        	    			if(!has.containsKey(result)){
        	    				has.put(result, null);
        	    			}
        	    			//System.out.println("detect: "+arr[8]);
        	    		}    	    		
        	    	}
    	    		if(Double.parseDouble(arr[0])>th[0]&Double.parseDouble(arr[1])>th[1]&Double.parseDouble(arr[2])>
    	    		th[2]&Double.parseDouble(arr[3])>th[3]){
    	    			result = arr[8];
    	    		}   	    		
                }
            }
            
            Iterator<String> iter = has.keySet().iterator();
        	while(iter.hasNext()){
        		    String tb = iter.next();
                	bufferWriter.write(tb);
                    bufferWriter.flush();
                    bufferWriter.newLine(); 
                //}
        	}
            bufferWriter.close();
            fout.close();
            bufferReader1.close();          
            fin1.close();
            bufferReader2.close();
            fin2.close();            
    	}catch (IOException e)
        {
       	 e.printStackTrace(); 
        }
    }
    
    void writePfile(String file,double th[]){
    	String result = "";
    	try{
    	    FileSystem fs = FileSystem.get(new JobConf());
	        FSDataOutputStream fout = fs.create(new Path(file));
	        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8")); 
	        for(int i=0;i<th.length;i++){
	        	result = new String(result+" "+th[i]);
	        }
	        bufferWriter.write(result);
            bufferWriter.flush();
            bufferWriter.close();fout.close();
    	    
    	}catch (IOException e)
        {
       	 e.printStackTrace(); 
        }    	
    }
    
    
    void htBurst(String file,String output,double HT){
    	
        int acc=0;
    	 //read and write
    	try{
    	    FileSystem fs = FileSystem.get(new JobConf());
   	        FSDataInputStream fin1 = fs.open(new Path(file));
   	        BufferedReader bufferReader1 = new BufferedReader(new InputStreamReader(fin1)); 
   	        FSDataInputStream fin2 = fs.open(new Path(file));
	        BufferedReader bufferReader2 = new BufferedReader(new InputStreamReader(fin2)); 
            if (fs.exists(new Path(output))){  
            	fs.delete(new Path(output),true);
            } 
            String line = null;
            //读参数文件
            while ((line= bufferReader1.readLine())!= null){
                 acc++;
            }
            //System.out.println(" acc1: "+acc);
            //heavy hitter to hashmap
            HashMap<String,Double> has=new HashMap<String,Double>();
            String ht[] = new String[acc];//从数组一个一个开始,带数量
            acc=0;
            while ((line= bufferReader2.readLine())!= null){           	
            	String array[] = line.split(" ");
            	String key = new String(array[0]+" "+array[1]+" "+array[2]);
            	double val = Double.parseDouble((array[3]));
            	ht[acc]=line;
            	has.put(key, val);
            	acc++;
            }
            //System.out.println(" acc2: "+acc);
            bufferReader1.close();          
            fin1.close();
            bufferReader2.close();
            fin2.close();
                        
	        FSDataOutputStream fout = fs.create(new Path(output));
	        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));  	        
            List<String> lst = new  ArrayList<String>(); 

	        //筛选
	        for (int i=0;i<acc;i++){
	        	String arr[] = ht[i].split(" ");	        	
	        	double curNum = Double.parseDouble(arr[3]);
	        	String lastHt = new String(arr[0]+" "+(Integer.valueOf(arr[1])-1)+" "+arr[2]);

	        	if(has.containsKey(lastHt)){
	        		//System.out.println(" has: "+lastHt);
	        		double num = has.get(lastHt);
	        		if(curNum>=2*num){
	        			lst.add(ht[i]);
	        		}
	        	}
	        	else if(curNum>2*HT){//上一时间片无heavy hitter
	        		lst.add(ht[i]);
	        	}
	        }
//output    //化简
            HashMap<String,String> res=new HashMap<String,String>();
	        for(int i=0;i<lst.size();i++){
	        	String result = lst.get(i);
	        	String arr[] = result.split(" ");
	        	String fd = arr[0].substring(5,6);
	        	//System.out.println("fd:"+fd);
	        	String nfd = new String(fd+ " "+arr[1]);
	        	if(res.containsKey(nfd)){
	        		res.put(nfd, new String(result+":"+res.get(nfd)));
	        	}else{
	        		res.put(nfd, result);
	        	}
	        }
	        
	        Iterator<String> iter = res.keySet().iterator();
        	while(iter.hasNext()){
        		String newk = iter.next();
        		String newline = new String(iter.next() + " " + res.get(newk));
	        	bufferWriter.write(newline);
                bufferWriter.flush();
                bufferWriter.newLine();	        	
	        }
                	
            bufferWriter.close();
            fout.close();
                        
    	}catch (IOException e)
        {
       	 e.printStackTrace(); 
        }
    }
	public static void main(String[] args) throws IOException { 
		
		//entropy parameters
		double interval = 10, filter = 1;
		double k = 1;
		double q = 0.5;
		double a = 4;
		double z = 10000;//default
		double DT = 0.01;//default
		double HT = 100;//default
		int rednum = 24;
		String sourceDir = null;
		if(args.length > 0)	sourceDir = args[0];

        //mapreduce out
		String midout1 = new String("/user/server/adProject/entromidout1");		
		String midout2 = new String("/user/server/adProject/entromidout2");	
		String output = new String("/user/server/adProject/TTHE");        
        //split out
		String midEntroFile = new String("/user/server/adProject/tmpEntroFile");
		String midHtFile = new String("/user/server/adProject/TTHEOut/tmpHtFile");
		//format entropy out
		String fEntroFile = new String("/user/server/adProject/TTHEOut");

		if(args.length > 1)	interval = Double.parseDouble(args[1]);
		if(args.length > 2)	q = Double.parseDouble(args[2]);
		if(args.length > 3)	a = Double.parseDouble(args[3]);
		if(args.length > 4)	z = Double.parseDouble(args[4]);
		if(args.length > 5)	DT = Double.parseDouble(args[5]);
		if(args.length > 6)	HT = Double.parseDouble(args[6]);
		if(args.length > 7)	filter = Double.parseDouble(args[7]);
		if(args.length > 8)	rednum = Integer.parseInt(args[8]);
		
		TTHE cpt = new TTHE();
		//cpt.writePfile(parameterFile, th);
		
	        org.apache.hadoop.conf.Configuration confThis = new JobConf(TTHE.class);
	        //set parameters
	        confThis.set("interval", String.valueOf(interval));
	        confThis.set("z", String.valueOf(z));
	        confThis.set("k", String.valueOf(k));
	        confThis.set("q", String.valueOf(q));
	        confThis.set("a", String.valueOf(a));
	        confThis.set("DT", String.valueOf(DT));
	        confThis.set("HT", String.valueOf(HT));
	        confThis.set("filter", String.valueOf(filter));
	        
	        Job job1 = new Job(confThis,"job1");		
	        Job job2 = new Job(confThis,"job1");
	        Job job3 = new Job(confThis,"job1");

		    job1.setJarByClass(TTHE.class);
		    job1.setNumReduceTasks(rednum);
		    
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(NullWritable.class);
	        job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(IntWritable.class);  
	        
		    job1.setMapperClass(Mapper01.class);
		    job1.setCombinerClass(Combiner01.class);
		    job1.setReducerClass(Reducer01.class);
		        
		    job1.setInputFormatClass(TextInputFormat.class);
		    job1.setOutputFormatClass(TextOutputFormat.class);
		    
		    if (sourceDir != null) FileInputFormat.setInputPaths(job1, new Path(sourceDir));
		    FileOutputFormat.setOutputPath(job1, new Path(midout1));
		    
		    
		    //Job2 
            job2.setJarByClass(TTHE.class);
            job2.setNumReduceTasks(rednum);
            
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(NullWritable.class);
	        job2.setMapOutputKeyClass(Text.class);
	        job2.setMapOutputValueClass(Text.class);
	        
		    job2.setMapperClass(Mapper02.class);
		    job2.setReducerClass(Reducer02.class);
		        
		    job2.setInputFormatClass(TextInputFormat.class);
		    job2.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(job2, new Path(midout1));
		    FileOutputFormat.setOutputPath(job2, new Path(midout2));
		
            //Job3 
		    
            job3.setJarByClass(TTHE.class);
            job3.setNumReduceTasks(1);
            
		    job3.setOutputKeyClass(Text.class);
		    job3.setOutputValueClass(NullWritable.class);
	        job3.setMapOutputKeyClass(Text.class);
	        job3.setMapOutputValueClass(LongWritable.class);
	        
	        
		    job3.setMapperClass(Mapper03.class);
		    job3.setReducerClass(Reducer03.class);
		        
		    job3.setInputFormatClass(TextInputFormat.class);
		    job3.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(job3, new Path(midout2));
		    FileOutputFormat.setOutputPath(job3, new Path(output));
        try {
	
			FileSystem fs1 = FileSystem.get(job1.getConfiguration());//读取到文件
			FileSystem fs2 = FileSystem.get(job2.getConfiguration());//读取到文件
			FileSystem fs3 = FileSystem.get(job3.getConfiguration());//读取到文件
			// delete any output that might exist from a previous run of this job
					        
	        if (fs1.exists(FileOutputFormat.getOutputPath(job1))) {
	           fs1.delete(FileOutputFormat.getOutputPath(job1), true);
	        }
	        if (fs2.exists(FileOutputFormat.getOutputPath(job2))) {		         
		       fs2.delete(FileOutputFormat.getOutputPath(job2), true);
		    }
	         
	        if (fs3.exists(FileOutputFormat.getOutputPath(job3))) {		         
		       fs3.delete(FileOutputFormat.getOutputPath(job3), true);
		    }
	        
	        if(job1.waitForCompletion(true)){
	        	if(job2.waitForCompletion(true)){
	        		if(job3.waitForCompletion(true)){//执行Job3	            
	                   fs1.delete(new Path(midout1), true);
	                   fs2.delete(new Path(midout2),true);        		
	        		}
	        	}
	        }               

        	String n="4";		    
            cpt.splitfile(output+"/part-r-00000",midEntroFile,midHtFile,n);
            cpt.finallyEntroFile(midEntroFile,fEntroFile,z);//分类

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			 //TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}