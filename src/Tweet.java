package src;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Tweet {
    private static final String DELIMETER = ";";
    private static final String TIMEZONE_ID = "America/Sao_Paulo";

    private final ZonedDateTime date;
    private final String id;
    private final String text;
    private final String device;

    private Tweet(ZonedDateTime date, String id, String text, String device) {
        this.date = date;
        this.id = id;
        this.text = text;
        this.device = device;
    }

    public static Optional<Tweet> fromCsvLine(String line) {
        List<String> columns = new ArrayList<>(Arrays.asList(line.split(DELIMETER)));

        if (columns.size() < 4) {
            return Optional.empty();
        }

        long epochTime;
        try {
            epochTime = Long.parseLong(columns.remove(0));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }

        String tweetId = columns.remove(0);
        String device = columns.remove(columns.size() - 1);
        String tweetText = String.join(DELIMETER, columns);

        ZonedDateTime date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochTime), ZoneId.of(TIMEZONE_ID));

        return Optional.of(new Tweet(date, tweetId, tweetText, device));
    }

    String getText() {
        return text;
    }

    ZonedDateTime getDate() {
        return date;
    }
}
