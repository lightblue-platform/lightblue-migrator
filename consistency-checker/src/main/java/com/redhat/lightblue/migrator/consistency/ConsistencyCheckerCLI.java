package com.redhat.lightblue.migrator.consistency;

import org.apache.commons.cli.*;

public final class ConsistencyCheckerCLI {

	private ConsistencyCheckerCLI() {

	}

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("name").withLongOpt("name").hasArg(true).withDescription("Name of checker instance").isRequired().create('n'));
		options.addOption(OptionBuilder.withArgName("hostname").withLongOpt("hostname").hasArg(true).withDescription("Hostname running the checker instance").isRequired().create('h'));
		options.addOption(OptionBuilder.withArgName("config").withLongOpt("config").hasArg(true).withDescription("Path to configuration file").isRequired().create('c'));
		options.addOption(OptionBuilder.withArgName("configversion").withLongOpt("configversion").hasArg(true).withDescription("migrationConfiguration Entity Version").isRequired().create('v'));
		options.addOption(OptionBuilder.withArgName("jobversion").withLongOpt("jobversion").hasArg(true).withDescription("migrationJob Entity Version").isRequired().create('j'));

		ConsistencyChecker checker = new ConsistencyChecker();
		PosixParser parser = new PosixParser();
		try {
			CommandLine commandline = parser.parse(options, args);

			Option[] opts = commandline.getOptions();
			for (Option opt : opts) {
				System.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
			}
			checker.setConsistencyCheckerName(System.getProperty("name"));
			checker.setHostName(System.getProperty("hostname"));
			checker.setConfigPath(System.getProperty("config"));

			ConsistencyChecker.LOGGER.info("Starting ConsistencyChecker");
			checker.execute();
			ConsistencyChecker.LOGGER.info("Finished ConsistencyChecker");
			System.exit(0);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(ConsistencyCheckerCLI.class.getSimpleName(), options, true);
			System.out.println("\n");
			System.out.println(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			ConsistencyChecker.LOGGER.error("Error running ConsistencyChecker", e);
			System.exit(1);
		}
	}
}
