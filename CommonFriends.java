import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

class Job1Mapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] persons = value.toString().split(" ");
        for (int i = 1; i < persons.length; i++)
            context.write(new Text(persons[i]), new Text(persons[0]));
    }
}

class Job1Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text person:values)
            sb.append(person.toString()).append(",");
        context.write(key, new Text(sb.toString()));
    }
}

class Job2Mapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        String friend = tokens[0];
        String[] users = tokens[1].split(",");
        Arrays.sort(users);
        int len = users.length;
        for (int i=0;i<len-1;i++){
            for (int j=i+1;j<len;j++)
                context.write(new Text(users[i]+"-"+users[j]), new Text(friend));
        }
    }
}

class Job2Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text person : values){
            sb.append(person).append(",");
        }
        String outStr = sb.toString().substring(0,sb.toString().length()-1);
        context.write(key, new Text(outStr));
    }
}

public class CommonFriends {

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf);
        job1.setJarByClass(CommonFriends.class);
        job1.setMapperClass(Job1Mapper.class);
        job1.setReducerClass(Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tmpDir = new Path(args[1] + "/tmp");
        FileOutputFormat.setOutputPath(job1, tmpDir);
        job1.waitForCompletion(true);

        Job job2 = new Job(conf);
        job2.setJarByClass(CommonFriends.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, tmpDir);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/result"));
        job2.waitForCompletion(true);
    }
}
