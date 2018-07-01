package src;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PartB2HashtagCountMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>
{
    private final IntWritable one = new IntWritable(1);

    private final static int MOST_POPULAR_HOUR = 22;

    private final static String HASHTAG_REGEX_SINGLE = "\\#\\d*_*\\p{L}\\w*";
    private final static String HASHTAG_REGEX_MULTIPLE = String.format("^(%s)|(?:\\s+)(%s)", HASHTAG_REGEX_SINGLE, HASHTAG_REGEX_SINGLE);

    private final static Pattern pattern = Pattern.compile(HASHTAG_REGEX_MULTIPLE, Pattern.UNICODE_CHARACTER_CLASS);

    private Text hashtag = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Optional opt = Tweet.fromCsvLine(value.toString());

        if (!opt.isPresent()) return;

        Tweet tweet = (Tweet)opt.get();

        if (tweet.getDate().getHour() != MOST_POPULAR_HOUR) return;

        Matcher matcher = pattern.matcher(tweet.getText());

        while (matcher.find()) {
            hashtag.set(matcher.group(matcher.group(1) != null ? 1 : 2).toLowerCase());
            context.write(hashtag, one);
        }
    }
}