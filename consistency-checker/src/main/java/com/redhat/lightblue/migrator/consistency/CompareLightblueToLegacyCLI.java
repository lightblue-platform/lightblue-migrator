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
		
		options.addOption(OptionBuilder.withArgName("lightblue-entity-name").withLongOpt("lightblue-entity-name").hasArg(true).withDescription("lightblue Entity Name").isRequired().create('l'));
		options.addOption(OptionBuilder.withArgName("lightblue-entity-version").withLongOpt("lightblue-entity-version").hasArg(true).withDescription("lightblue Entity Version").isRequired().create('v'));
		options.addOption(OptionBuilder.withArgName("legacy-entity-name").withLongOpt("legacy-entity-name").hasArg(true).withDescription("User DN").isRequired().create('g'));
		options.addOption(OptionBuilder.withArgName("legacy-entity-version").withLongOpt("legacy-entity-version").hasArg(true).withDescription("LDAP Password").isRequired().create('s'));
		options.addOption(OptionBuilder.withArgName("lightblue-service-uri").withLongOpt("lightblue-service-uri").hasArg(true).withDescription("lightblue Service URI").isRequired().create('u'));
		options.addOption(OptionBuilder.withArgName("legacy-service-uri").withLongOpt("legacy-service-uri").hasArg(true).withDescription("Legacy Service URI").isRequired().create('i'));
		options.addOption(OptionBuilder.withArgName("legacy-find-json").withLongOpt("legacy-find-json").hasArg(true).withDescription("Legacy JSON Find Expression").isRequired().create('j'));
		options.addOption(OptionBuilder.withArgName("lightblue-find-json").withLongOpt("lightblue-find-json").hasArg(true).withDescription("lightblue JSON Find Expression").isRequired().create('s'));
		options.addOption(OptionBuilder.withArgName("lightblue-save-json").withLongOpt("lightblue-save-json").hasArg(true).withDescription("lightblue JSON Save Expression").isRequired().create('n'));
		options.addOption(OptionBuilder.withArgName("overwrite-lightblue").withLongOpt("overwrite-lightblue").hasArg(true).withDescription("Overwrite lightblue documents").isRequired().create('o'));
	
		CompareLightblueToLegacyCommand command = new CompareLightblueToLegacyCommand();
		PosixParser parser = new PosixParser();
		try {
			CommandLine commandline = parser.parse(options, args);
			
			Option[] opts = commandline.getOptions();
			for (Option opt : opts) {
				System.setProperty(opt.getLongOpt(), opt.getValue() == null ? "true" : opt.getValue());
			}

			command.setLightblueEntityName(System.getProperty("lightblue-entity-name"));
			command.setLightblueEntityVersion(System.getProperty("lightblue-entity-version"));
			command.setLegacyEntityName(System.getProperty("legacy-entity-name"));
			command.setLegacyEntityVersion(System.getProperty("legacy-entity-version"));
			command.setLightblueServiceURI(System.getProperty("lightblue-service-uri"));
			command.setLegacyServiceURI(System.getProperty("legacy-service-uri"));
			command.setLegacyFindJsonExpression(System.getProperty("legacy-find-json"));
			command.setLightblueFindJsonExpression(System.getProperty("lightblue-find-json"));
			command.setLightblueSaveJsonExpression(System.getProperty("lightblue-save-json"));
			
			command.setOverwriteLightblueDocuments("true".equals(System.getProperty("overwrite-lightblue")) ? true : false);
			
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
