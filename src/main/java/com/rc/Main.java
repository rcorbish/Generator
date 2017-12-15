package com.rc;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.config.Config;
import com.rc.config.Parser;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Main class starts the app
 * 
 * Use --help to see command line options
 * 
 * If an exec is given the process executes it, then terminates
 * If no execs are provided, the app starts the web service
 * and waits for triggers to fire to initiate execution
 */
public class Main {
	final private static Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		Options options = new Options(args);
		if( options.helped ) return ;
		try {
			Map<String, Processor> processors = new LinkedHashMap<>();

			// get config
			Parser parser = new Parser();
			List<Config> configs = parser.parse(options.configDir);

			// create Sinks, Sources & Processors
			for (Config config : configs) {
				Processor processor = config.createProcessor();
				processors.put(processor.name, processor);
			}

			if (options.execProcesses != null) {
				log.info("Executing requested processes immediately");
				ExecutorService es = Executors.newFixedThreadPool(options.numberThreads);

				for (String processName : options.execProcesses) {
					Processor p = processors.get(processName);
					if (p != null) {
						es.submit(() -> p.fire(null));
					} else {
						log.warn("Cannot find requested process {}", processName);
					}
				}
				es.shutdown();
				es.awaitTermination(30, TimeUnit.MINUTES);

			} else {
				@SuppressWarnings("resource")
				Monitor m = new Monitor();
				m.start(options.port);
				for (Processor p : processors.values()) {
					p.start();
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(2);
		}
	}
}


/**
 * Private class to hold the command line options passed in
 * at runtime.
 */
class Options {

	int port = 8111;
	Path configDir = null;
	String execProcesses[];
	int numberThreads = 1;

	final boolean helped ;
	
	public Options(String args[]) {
		OptionParser parser = new OptionParser();
		parser.accepts("port", "Port number for website, default 8111").withRequiredArg().ofType(Integer.class);
		parser.accepts("config-dir", "Directory for config files").withRequiredArg().ofType(String.class);
		;
		parser.accepts("exec", "Request immediate execution of a named process (repeatable)").withRequiredArg().ofType(String.class);
		;
		parser.accepts("concurrency", "Number of concurrent processors, default=1").withRequiredArg().ofType(Integer.class);
		;
        parser.accepts( "help", "This help" ).forHelp();
        
		OptionSet os = parser.parse(args);
		if (os.has("port")) {
			port = (Integer) os.valueOf("port");
		}
		if (os.has("concurrency")) {
			numberThreads = (Integer) os.valueOf("concurrency");
		}
		if (os.has("config-dir")) {
			configDir = Paths.get((String) os.valueOf("config-dir"));
		} else {
			configDir = Paths.get(".");
		}
		if (os.has("exec")) {
			List<?> tmp = os.valuesOf("exec");
			execProcesses = new String[tmp.size()];
			tmp.toArray(execProcesses);
		} else {
			execProcesses = null;
		}
		
		helped = os.has( "help") ;
		if( helped ) {
			try {
				parser.printHelpOn( System.out );
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}