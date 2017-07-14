package com.example.h2performance;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;

import static java.sql.Timestamp.from;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.stream.IntStream.range;
import static org.springframework.test.jdbc.JdbcTestUtils.countRowsInTable;
import static org.springframework.test.jdbc.JdbcTestUtils.deleteFromTables;

@RunWith(SpringRunner.class)
@SpringBootTest
public class H2PerformanceApplicationTests {

    @Autowired NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    @Autowired JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {

        final int recordsCount = 1_000_000;

        if (countRowsInTable(jdbcTemplate, "log") == recordsCount) {
            return;
        }

        deleteFromTables(jdbcTemplate, "log");

        Duration duration = time(() -> {

            String randomString = randomString(1024);

            range(0, recordsCount).forEach(value -> {
                MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
                    .addValue("timestamp", from(now()))
                    .addValue("entry", randomString);

                namedParameterJdbcTemplate.update(
                    "insert into log (timestamp, entry) values (:timestamp, :entry)",
                    mapSqlParameterSource);
            });
        });
        log("CREATED {0} records in {1}.", recordsCount, format(duration));
    }

    @Test
    public void contextLoads() {

        final Data data = new Data();

        Duration duration = time(() -> {
            data.setEarliestTimestamp(namedParameterJdbcTemplate.queryForObject("select min(timestamp) from log", new
                MapSqlParameterSource(), Timestamp.class));
        });
        log("FOUND earliest timestamp in {0}.", format(duration));


        duration = time(() -> {
            data.setLatestTimestamp(namedParameterJdbcTemplate.queryForObject("select max(timestamp) from log", new
                MapSqlParameterSource(), Timestamp.class));
        });
        log("FOUND latest timestamp in {0}.", format(duration));



        log("Earliest timestamp: {0} ", format(data.getEarliestTimestamp()));
        log("Latest timestamp: {0} ", format(data.getLatestTimestamp()));



        Timestamp tenSecAfterStart = new Timestamp(data.getEarliestTimestamp().getTime() + 1000 * 10);
        Timestamp tenSecBeforeEnd = new Timestamp(data.getLatestTimestamp().getTime() - 1000 * 10);

        duration = time(() -> {
            data.setCount(namedParameterJdbcTemplate.queryForObject(
                "select count(*) from log where timestamp > :low and timestamp < :high",
                new MapSqlParameterSource()
                    .addValue("low", tenSecAfterStart)
                    .addValue("high", tenSecBeforeEnd)
                , Long.class));
        });
        log("FOUND {0} matching records in {1}.", data.getCount(), format(duration));


    }

    private String format(final Duration duration) {
        return format(duration.toMillis());
    }

    private String format(final Timestamp timestamp) {
        return format(timestamp.getTime());
    }

    private String format(final long millis) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("mm:ss:SSS");
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId
            .systemDefault()).format(dateTimeFormatter);
    }

    private Duration time(Task task) {
        Instant start = Instant.now();

        task.execute();

        Instant stop = Instant.now();

        return between(start, stop);
    }

    private void log(final String pattern, final Object... args) {
        System.out.println(MessageFormat.format(pattern, args));
    }


    private String randomString(int length) {
        return range(0, length).collect(StringBuilder::new, (stringBuilder, value) -> stringBuilder.append(value),
            StringBuilder::append).toString();
    }

    private interface Task {
        void execute();
    }

    private static class Data {
        private Timestamp earliestTimestamp;
        private Timestamp latestTimestamp;
        private long count;

        private Timestamp getEarliestTimestamp() {
            return earliestTimestamp;
        }

        private void setEarliestTimestamp(final Timestamp earliestTimestamp) {
            this.earliestTimestamp = earliestTimestamp;
        }

        private Timestamp getLatestTimestamp() {
            return latestTimestamp;
        }

        private void setLatestTimestamp(final Timestamp latestTimestamp) {
            this.latestTimestamp = latestTimestamp;
        }

        private void setCount(final long count) {
            this.count = count;
        }

        private long getCount() {
            return count;
        }
    }
}
