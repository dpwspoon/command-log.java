/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.command.log.internal.post.process;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.util.regex.Pattern.compile;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.HdrHistogram.Histogram;

public class PostProcessLatencyCommand
{

    private static final long HISTOGRAM_LARGEST_TIME_MILLIS = 360000000L;

    Pattern pattern = compile("(?<timestamp>\\d+),\\s" +
                              "(?<traceId>0x\\S+),\\s" +
                              "(?<nukleus1>\\S+)\\s" +
                              "(?<direction>->|<-)\\s" +
                              "(?<nukleus2>\\S+)," +
                              ".*");

    Map<String, Histogram> latencyByNukleus;
    Map<String, SortedMap<Long, Latency>> traceIdToLatencyByNukleus;

    public PostProcessLatencyCommand(
        String inputFile,
        String outputFile) throws Exception
    {
        latencyByNukleus = new HashMap<String, Histogram>();
        traceIdToLatencyByNukleus = new HashMap<String, SortedMap<Long, Latency>>();

        try (Stream<String> stream = Files.lines(Paths.get(inputFile)))
        {
            stream.forEachOrdered(this::handleLine);
        }

        traceIdToLatencyByNukleus.values().stream().forEach(v1 -> v1.values().stream().forEach(v2 -> v2.recordIfComplete()));
        latencyByNukleus.entrySet().stream().forEach(this::printHistogram);
    }

    private void handleLine(String line)
    {
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches())
        {
            Long timestamp = Long.parseLong(matcher.group("timestamp"));
            Long traceId = Long.decode(matcher.group("traceId"));
            boolean leftToRight = "->".equals(matcher.group("direction"));
            String from = leftToRight ? matcher.group("nukleus1") : matcher.group("nukleus2");
            String to = leftToRight ? matcher.group("nukleus2") : matcher.group("nukleus1");
            handleFrame(timestamp, traceId, from, to);
        }
        else
        {
            System.out.println("Failed to match input, potential bug: \"" + line + "\"");
            System.exit(2);
        }
    }

    private void handleFrame(
        Long timestamp,
        Long traceId,
        String from,
        String to)
    {
        if (traceId != 0)
        {

            SortedMap<Long, Latency> toLatencyMap = traceIdToLatencyByNukleus.computeIfAbsent(to, this::initMap);
            Latency toLatency = toLatencyMap.compute(traceId, (k, v) -> v == null ? new Latency(initHistogram(to)) :
                                                                                    new Latency(v.recordIfComplete()));
            toLatency.setStart(timestamp);

            SortedMap<Long, Latency> fromLatencyMap = traceIdToLatencyByNukleus.computeIfAbsent(from, this::initMap);
            Latency fromLatency = fromLatencyMap.computeIfAbsent(traceId, t -> new Latency(initHistogram(from)));
            fromLatency.setEnd(timestamp);

        }
    }

    private SortedMap<Long, Latency> initMap(String nukleus)
    {
        return new TreeMap<Long, Latency>();
    }

    private Histogram initHistogram(String nukleus)
    {
        return latencyByNukleus.computeIfAbsent(nukleus, x -> new Histogram(HISTOGRAM_LARGEST_TIME_MILLIS, 5));
    }

    private void printHistogram(Map.Entry<String, Histogram> entry)
    {
        System.out.println("======= " + entry.getKey() + " =======");
        entry.getValue().outputPercentileDistribution(System.out, 1.0);
        System.out.println();
    }

    class Latency
    {
        private long start = MAX_VALUE;
        private long end = MIN_VALUE;
        private Histogram histogram;

        Latency(Histogram histogram)
        {
            this.histogram = histogram;
        }

        Histogram recordIfComplete()
        {
            if (end != MIN_VALUE && start != MAX_VALUE)
            {
                histogram.recordValue(end - start);
            }
            return histogram;
        }

        void setStart(long newStart)
        {
            if (newStart < start)
            {
                start = newStart;
            }
        }

        void setEnd(long newEnd)
        {
            if (end < newEnd)
            {
                end = newEnd;
            }
        }
    }
}
