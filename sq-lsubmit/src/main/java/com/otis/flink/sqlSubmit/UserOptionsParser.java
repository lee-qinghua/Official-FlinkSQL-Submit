package com.otis.flink.sqlSubmit;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;

public class UserOptionsParser {
    //目前只设置一个参数
    public static final Option OPTION_SQL_DIR = Option
            .builder("sdir")
            .required(true)
            .longOpt("the sql location")
            .numberOfArgs(1)
            .argName("the sql location")
            .desc("the sql location")
            .build();

    public static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    public static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_DIR);
        return options;
    }

    public static UserOptions parseUserArgs(String[] args) {
        Preconditions.checkArgument(args.length<1, "Please input -sdir <the sql location>");
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new UserOptions(
                    line.getOptionValue(UserOptionsParser.OPTION_SQL_DIR.getOpt())
            );
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
