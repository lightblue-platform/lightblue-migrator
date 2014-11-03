package com.redhat.lightblue.migrator.consistency;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public final class CompareLightblueToLegacyCLI {
	
	private CompareLightblueToLegacyCLI() {
		
	}
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("lbentity").withLongOpt("lightblueentityname").hasArg(true).withDescription("lightblue Entity Name").isRequired().create('l'));
		options.addOption(OptionBuilder.withArgName("lbversion").withLongOpt("lightblueentityversion").hasArg(true).withDescription("lightblue Entity Version").isRequired().create('v'));
		options.addOption(OptionBuilder.withArgName("legentity").withLongOpt("legacyentityname").hasArg(true).withDescription("User DN").isRequired().create('g'));
		options.addOption(OptionBuilder.withArgName("legversion").withLongOpt("legacyentityversion").hasArg(true).withDescription("LDAP Password").isRequired().create('s'));

		options.addOption(OptionBuilder.withArgName("lburi").withLongOpt("lightblueserviceuri").hasArg(true).withDescription("lightblue Service URI").isRequired().create('u'));
		options.addOption(OptionBuilder.withArgName("lguri").withLongOpt("legacyserviceuri").hasArg(true).withDescription("Legacy Service URI").isRequired().create('i'));
		options.addOption(OptionBuilder.withArgName("lgfjson").withLongOpt("legacyfindjson").hasArg(true).withDescription("Legacy JSON Find Expression").isRequired().create('j'));
		options.addOption(OptionBuilder.withArgName("lbfjson").withLongOpt("lightbluefindjson").hasArg(true).withDescription("lightblue JSON Find Expression").isRequired().create('s'));
		options.addOption(OptionBuilder.withArgName("lbujson").withLongOpt("lightblueupdatejson").hasArg(true).withDescription("lightblue JSON Update Expression").isRequired().create('n'));
				
		options.addOption(OptionBuilder.withArgName("ov").withLongOpt("overwritelightblue").hasArg(true).withDescription("Overwrite lightblue documents").isRequired().create('o'));
		
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand();
		PosixParser parser = new PosixParser();
		try {
			CommandLine commandline = parser.parse(options, args);
			
			Option[] opts = commandline.getOptions();
			for (Option opt : opts) {
				System.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
			}

			command.setLightblueEntityName(System.getProperty("lightblueentityname"));
			command.setLightblueEntityVersion(System.getProperty("lightblueentityversion"));
			command.setLegacyEntityName(System.getProperty("legacyentityname"));
			command.setLegacyEntityVersion(System.getProperty("legacyentityversion"));
			command.setLightblueServiceURI(System.getProperty("lburi"));
			command.setLegacyServiceURI(System.getProperty("lguri"));
			command.setLegacyFindJsonExpression(System.getProperty("legacyfindjson"));
			command.setLightblueFindJsonExpression(System.getProperty("lightbluefindjson"));
			command.setLightblueUpdateJsonExpression(System.getProperty("lightblueupdatejson"));
			
			command.setOverwriteLightblueDocuments("true".equals(System.getProperty("overwritelightblue")) ? true : false);
			
			CompareLightblueToLegacyCommand.LOG.info("Starting CompareLightblueToLegacyCommand");
			command.execute();
			CompareLightblueToLegacyCommand.LOG.info("Finished CompareLightblueToLegacyCommand");
			logCommandResults(command);
			if(command.hasFailures()) {
				System.exit(1);
			} else {
				System.exit(0);	
			}
			
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( CompareLightblueToLegacyCLI.class.getSimpleName(),options ,true);
			System.out.println("\n");
			System.out.println(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			logCommandResults(command);
			CompareLightblueToLegacyCommand.LOG.error("Error processing CompareLightblueToLegacyCommand", e);
			System.exit(1);
		}
		
	}
	
	private static void logCommandResults(CompareLightblueToLegacyCommand c) {
		if (c != null) {
			CompareLightblueToLegacyCommand.LOG.info("The number of inconsistent documents was " + c.getInconsistentDocuments());
			CompareLightblueToLegacyCommand.LOG.info("The number of documents compared was " + c.getDocumentsCompared());
			CompareLightblueToLegacyCommand.LOG.info("The number of lightblue documented that were updated/overwritten was " + c.getLightblueDocumentsUpdated());
		}
	}
	
	
}
