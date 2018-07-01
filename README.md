# Twitter analysis in Hadoop

Hadoop implementation of tweet length and most common hashtag analysis.
This was a coursework assignment based on Twitter data collected during the 2016 Olympics in Rio.

Assumes that the tweet input file contains semicolon separated values with the following columns:

```
epoch_time;tweetId;tweet(including #hashtags);device

```

For example:

```
1530473325000;123456789;I like big #dogs;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>
```

The athletes input file is the one collected from https://www.kaggle.com/rio2016/olympic-games/data.

## Jobs

### PartAJob

Generates a distribution of tweet lengths, grouped into buckets of 5 (for use in histogram plots).
Full UTF-8 support for length calculation is included, just like Twitter's own implementation.

### ParB1Job

Computes the number of tweets that occurred during each hour in the dataset.
It will generate at most 24 outputs (days are not respected).

### PartB2Job

For the most popular hour found in `PartB1Job`, emits and saves to HDFS the top 10 hashtags tweeted
during that hour. Full UTF-8 support for hashtag parsing is included, just like Twitter's own implementation.

### PartC1Job

Computes the top 30 athletes from the Kaggle dataset, based on the number of mentions.
Includes the number of mentions in the output.

### PartC2Job

Computes the top 20 sports from the Kaggle dataset, based on the number of mentions.
Includes the number of mentions in the output.
