import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AgeCount{

public static class AgeMapper extends Mapper<LongWritable,Text,Text,Text>{
Text finalKey = new Text();
String ageGroup;
@Override
public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
String str = value.toString();
String[] strList = str.split(",");
String yearCode  = strList[0];
String year = yearCode.substring(0,4);
try{
if(strList[0]!=null){
int age = Integer.parseInt(strList[8]);
if(range(age,0,9)){
ageGroup = "0-9";	
}else if(range(age,10,19)){
ageGroup =  "10-19";	
}else if(range(age,20,29)){
ageGroup =  "20-29";	
}else if(range(age,30,39)){
ageGroup =  "30-39";	
}else if(range(age,40,49)){
ageGroup =  "40-49";	
}else if(range(age,50,59)){
ageGroup =  "50-59";	
}else if(range(age,60,69)){
ageGroup =  "60-69";	
}else if(range(age,70,79)){
ageGroup =  "70-79";	
}else if(range(age,80,89)){
ageGroup =  "80-89";	
}else if(range(age,90,99)){
ageGroup =  "90-99";	
}
finalKey.set(new Text(year)+"("+new Text(ageGroup)+")");
c.write(finalKey,new Text("1"));
}
}
catch(NumberFormatException e) {
	System.out.println(e);
}
}
}
public static boolean range(int x, int lower, int upper){
return (lower<=x && x<=upper);	
}

public static class AgeReducer extends Reducer<Text,Text,Text,Text>{
@Override
public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{
int count = 0;
for(Text val:values){
count += 1;
}
c.write(key, new Text(""+count));
}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
Configuration conf = new Configuration();
Job j2 = new Job(conf);
j2.setJobName("AgeCount job");
j2.setJarByClass(AgeCount.class);
//Mapper input and output
j2.setMapOutputKeyClass(Text.class);
j2.setMapOutputValueClass(Text.class);
//Reducer input and output
j2.setOutputKeyClass(Text.class);
j2.setOutputValueClass(Text.class);
//file input and output of the whole program
j2.setInputFormatClass(TextInputFormat.class);
j2.setOutputFormatClass(TextOutputFormat.class);
//Set the mapper class
j2.setMapperClass(AgeMapper.class);
//set the combiner class for custom combiner
//j2.setCombinerClass(WordReducer.class);
//Set the reducer class
j2.setReducerClass(AgeReducer.class);
//set the number of reducer if it is zero means there is no reducer
//j2.setNumReduceTasks(0);
FileOutputFormat.setOutputPath(j2, new Path(args[1]));
FileInputFormat.addInputPath(j2, new Path(args[0]));
j2.waitForCompletion(true);
}
	
}