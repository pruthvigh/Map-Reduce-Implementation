import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Assignment {
//mapper
public static class SalaryMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

private Text finalKey = new Text();
String state="Undefined";
String gender;
@Override
public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
String str = value.toString();
String[] strList = str.split(",");
//getting year code 
String yearCode  = strList[0];
String year = yearCode.substring(0,4);
//getting gender code
String genderCode = strList[69];
if(genderCode.equals("1")){
gender = "Male";	
}else{
gender = "Female";	
}
try {
	//setting state name
String stateCode = strList[5];
if(Integer.parseInt(stateCode)==1){
state = "Alabama";	
}else if(Integer.parseInt(stateCode)==2){
state = "Alaska";	
}else if(Integer.parseInt(stateCode)==4){
state = "Arizona";	
}else if(Integer.parseInt(stateCode)==5){
state = "Arkansas";	
}else if(Integer.parseInt(stateCode)==6){
state = "California";	
}else if(Integer.parseInt(stateCode)==8){
state = "Colorado";	
}else if(Integer.parseInt(stateCode)==9){
state = "Connecticut";	
}else if(Integer.parseInt(stateCode)==10){
state = "Delaware";	
}else if(Integer.parseInt(stateCode)==11){
state = "District of Columbia";	
}else if(Integer.parseInt(stateCode)==12){
state = "Florida";	
}else if(Integer.parseInt(stateCode)==13){
state = "Georgia";	
}else if(Integer.parseInt(stateCode)==15){
state = "Hawaii";	
}else if(Integer.parseInt(stateCode)==16){
state = "Idaho";	
}else if(Integer.parseInt(stateCode)==17){
state = "Illinois";	
}else if(Integer.parseInt(stateCode)==18){
state = "Indiana";	
}else if(Integer.parseInt(stateCode)==19){
state = "Iowa";	
}else if(Integer.parseInt(stateCode)==20){
state = "kansas";	
}else if(Integer.parseInt(stateCode)==21){
state = "Kentucky";	
}else if(Integer.parseInt(stateCode)==22){
state = "Louisana";	
}else if(Integer.parseInt(stateCode)==23){
state = "Maine";	
}else if(Integer.parseInt(stateCode)==24){
state = "Maryland";	
}else if(Integer.parseInt(stateCode)==25){
state = "Massachussets";	
}else if(Integer.parseInt(stateCode)==26){
state = "Michigan";	
}else if(Integer.parseInt(stateCode)==27){
state = "Minnesota";	
}else if(Integer.parseInt(stateCode)==28){
state = "Mississippi";	
}else if(Integer.parseInt(stateCode)==29){
state = "Missouri";	
}else if(Integer.parseInt(stateCode)==30){
state = "Montana";	
}else if(Integer.parseInt(stateCode)==31){
state = "Nebraska";	
}else if(Integer.parseInt(stateCode)==32){
state = "Nevada";	
}else if(Integer.parseInt(stateCode)==33){
state = "New Hampshire";	
}else if(Integer.parseInt(stateCode)==34){
state = "New Jersey";	
}else if(Integer.parseInt(stateCode)==35){
state = "New Mexico";	
}else if(Integer.parseInt(stateCode)==36){
state = "New York";	
}else if(Integer.parseInt(stateCode)==37){
state = "North Carolina";	
}else if(Integer.parseInt(stateCode)==38){
state = "North Dakota";	
}else if(Integer.parseInt(stateCode)==39){
state = "Ohio";	
}else if(Integer.parseInt(stateCode)==40){
state = "Oklahoma";	
}else if(Integer.parseInt(stateCode)==41){
state = "Oregon";	
}else if(Integer.parseInt(stateCode)==42){
state = "Pennsylvania";	
}else if(Integer.parseInt(stateCode)==44){
state = "Rhode Island";	
}else if(Integer.parseInt(stateCode)==45){
state = "South Carolina";	
}else if(Integer.parseInt(stateCode)==46){
state = "South Dakota";	
}else if(Integer.parseInt(stateCode)==47){
state = "Tennessee";	
}else if(Integer.parseInt(stateCode)==48){
state = "Texas";	
}else if(Integer.parseInt(stateCode)==49){
state = "Utah";	
}else if(Integer.parseInt(stateCode)==50){
state = "Vermont";	
}else if(Integer.parseInt(stateCode)==51){
state = "Virginia";	
}else if(Integer.parseInt(stateCode)==53){
state = "Washington";	
}else if(Integer.parseInt(stateCode)==54){
state = "West Virginia";	
}else if(Integer.parseInt(stateCode)==55){
state = "Wisconsin";	
}else if(Integer.parseInt(stateCode)==56){
state = "Wyoming";	
}else if(Integer.parseInt(stateCode)==72){
state = "Puerto Rico";	
}
//getting salary
String salString = strList[72];
if(isInteger(salString) && ((Integer.parseInt(salString))>=1)&&((Integer.parseInt(salString))<=999999)){
	long salary = Long.parseLong(salString);
System.out.println("year "+year+"state "+state+"gender "+gender);
finalKey.set(new Text(year)+"_"+new Text(state)+"_"+new Text(gender));
//emit each key with corresponding salary
c.write(finalKey, new LongWritable(salary));
}
}
catch(NumberFormatException e) {
	System.out.println(e);
}
}
//Method for checking whether string is an integer
public static boolean isInteger(String s) {
    try { 
        Integer.parseInt(s); 
    } catch(NumberFormatException e) { 
        return false; 
    } catch(NullPointerException e) {
        return false;
    }
    // only got here if we didn't return false
    return true;
}
}
//reducer
public static class SalaryReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
@Override
public void reduce(Text key, Iterable<LongWritable>values, Context c) throws IOException,InterruptedException{
long sumSal = 0;
int count = 0;
for(LongWritable val:values){
 sumSal += val.get();
 count++;
}
sumSal = sumSal/count;
 c.write(key, new LongWritable(sumSal));
}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
Configuration conf = new Configuration();
//comment below line if you want to go with the default split size of 128 MB
conf.set("mapreduce.input.fileinputformat.split.maxsize","20971520");
Job j2 = new Job(conf,"Assignment");
j2.setJobName("Assignment job");
j2.setJarByClass(Assignment.class);
//Mapper input and output
j2.setMapOutputKeyClass(Text.class);
j2.setMapOutputValueClass(LongWritable.class);
//Reducer input and output
j2.setOutputKeyClass(Text.class);
j2.setOutputValueClass(LongWritable.class);
//file input and output of the whole program
j2.setInputFormatClass(TextInputFormat.class);
j2.setOutputFormatClass(TextOutputFormat.class);
//Set the mapper class
j2.setMapperClass(SalaryMapper.class);
//j2.setNumMapTasks(70);
//set the combiner class for custom combiner
//j2.setCombinerClass(WordReducer.class);
//Set the reducer class
j2.setReducerClass(SalaryReducer.class);
//set the number of reducer if it is zero means there is no reducer
//j2.setNumReduceTasks(0);
FileOutputFormat.setOutputPath(j2, new Path(args[1]));
FileInputFormat.addInputPath(j2, new Path(args[0]));
j2.waitForCompletion(true);
}
}
