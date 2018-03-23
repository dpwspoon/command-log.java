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

import static org.apache.commons.cli.Option.builder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class PostProcess
{
    public static void main(String[] args) throws Exception
    {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(builder("h").longOpt("help").desc("print this message").build());
        options.addOption(builder("t").hasArg()
                .required(false)
                .longOpt("type")
                .desc("sort* | latency-histogram")
                .build());
        options.addOption(builder("if").longOpt("input-file").hasArg().desc("input-file").build());
        options.addOption(builder("of").longOpt("output-file").hasArg().desc("output-file (defaults to inplace)").build());

        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("help"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("log", options);
        }
        else
        {
            String type = cmdline.getOptionValue("type", "sort");
            String inputFile = cmdline.getOptionValue("input-file");
            String outputFile = cmdline.getOptionValue("output-file", inputFile);
            if ("sort".equals(type))
            {
                throw new RuntimeException("Not yet implemented");
            }
            else if("latency-histogram".equals(type))
            {
                System.out.println("This command currently requires a sorted file");
                new PostProcessLatencyCommand(inputFile, outputFile);
            }
        }
    }
}
