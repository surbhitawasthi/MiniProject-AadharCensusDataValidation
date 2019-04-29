import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AadharCensusReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int m_count = 0, f_count = 0;
        for(Text value:values)
        {
           if( (value.toString()).equals("F") )
               f_count++;
           else if ( (value.toString()).equals("M") )
               m_count++;
        }
        String finalResult = "M: "+m_count+" F: "+f_count;

        context.write(key, new Text(finalResult));
    }
}
