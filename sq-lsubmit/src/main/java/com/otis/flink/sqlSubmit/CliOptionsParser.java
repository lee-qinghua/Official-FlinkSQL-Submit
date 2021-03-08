package com.otis.flink.sqlSubmit;

import org.apache.commons.cli.*;

public class CliOptionsParser {

    public static final Option OPTION_WORKING_SPACE = Option
            .builder("w")
            .required(true)
            .longOpt("working_space")
            .numberOfArgs(1)
            .argName("working space dir")
            .desc("The working space dir.")
            .build();

    public static final Option OPTION_SQL_FILE = Option
            .builder("f")
            .required(true)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("SQL file path")
            .desc("The SQL file path.")
            .build();

    public static final Option OPTION_HIVE_CONF = Option
            .builder("hc")
            .required(true)
            .longOpt("hive_conf")
            .numberOfArgs(1)
            .argName("Hive config file path")
            .desc("The Hive config path.")
            .build();

    public static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    public static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_WORKING_SPACE);
//        options.addOption(OPTION_HIVE_CONF);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static CliOptions parseClient(String[] args) {
        if (args.length < 1) {
            //throw new RuntimeException("Please input -w <work_space_dir> -f <sql-file> -hc <hive-conf>");
            throw new RuntimeException("Please input -w <work_space_dir> -f <sql-file>");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(
                    line.getOptionValue(CliOptionsParser.OPTION_SQL_FILE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_WORKING_SPACE.getOpt())
                    //line.getOptionValue(CliOptionsParser.OPTION_HIVE_CONF.getOpt())
            );
        }
        catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}