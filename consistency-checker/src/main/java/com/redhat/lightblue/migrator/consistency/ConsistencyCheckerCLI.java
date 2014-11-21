package com.redhat.lightblue.migrator.consistency;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public final class ConsistencyCheckerCLI {
	
	private ConsistencyCheckerCLI() {
		
	}
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("name").withLongOpt("name").hasArg(true).withDescription("Name of checker instance").isRequired().create('n'));
		options.addOption(OptionBuilder.withArgName("hostname").withLongOpt("hostname").hasArg(true).withDescription("Hostname running the checker instance").isRequired().create('h'));
		options.addOption(OptionBuilder.withArgName("ip").withLongOpt("ip").hasArg(true).withDescription("IP address of host running the checker instance").isRequired().create('i'));
		
		ConsistencyChecker checker = new ConsistencyChecker();
		PosixParser parser = new PosixParser();
		try {
			CommandLine commandline = parser.parse(options, args);
			
			Option[] opts = commandline.getOptions();
			for (Option opt : opts) {
				System.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
			}
			checker.setCheckerName(System.getProperty("name"));
			checker.setHostname(System.getProperty("hostname"));
			checker.setIpAddress(System.getProperty("ip"));
			
			ConsistencyChecker.LOG.info("Starting ConsistencyChecker");
			checker.execute();
			ConsistencyChecker.LOG.info("Finished ConsistencyChecker");

			if(checker.hasFailures()) {
				System.exit(1);
			} else {
				System.exit(0);	
			}
			
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( ConsistencyCheckerCLI.class.getSimpleName(),options ,true);
			System.out.println("\n");
			System.out.println(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			ConsistencyChecker.LOG.error("Error running ConsistencyChecker", e);
			System.exit(1);
		}
		
	}
	
}
