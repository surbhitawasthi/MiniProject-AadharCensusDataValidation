import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AadharCensusMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String inputLine[] = value.toString().split(",");

        String nameOfState = inputLine[2];
        String gender = inputLine[6];
        int generated = Integer.parseInt(inputLine[8]);

//        System.out.println("nameOfState: "+nameOfState+"\tgender: "+gender);

        if(!(nameOfState.isEmpty() || gender.isEmpty()) && generated == 1)
            context.write(new Text(nameOfState), new Text(gender));
    }
}
