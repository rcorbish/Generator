package com.rc;

import java.io.File;
import java.net.URL;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * This handles the web pages. 
 * 
 * We use spark to serve pages. It's simple and easy to configure. 
 * 
 * @author richard
 *
 */
public class Monitor implements AutoCloseable {
	
	final static Logger logger = LoggerFactory.getLogger( Monitor.class ) ;

	final Random random ;
	final Gson gson ;

	public Monitor() {
		this.random = new Random( ) ;
		gson = new GsonBuilder()
				.setPrettyPrinting()
				.create() ;
	} 
	
	 
	public void start( int port ) {
		try {			
			spark.Spark.port( port ) ;
			URL mainPage = getClass().getClassLoader().getResource( "index.html" ) ;
			File path = new File( mainPage.getPath() ) ;
			spark.Spark.staticFiles.externalLocation( path.getParent() ) ;

			spark.Spark.get( "/app", (req,rsp) -> { return "Hello" ; } ) ;
			spark.Spark.awaitInitialization() ;
		} catch( Exception ohohChongo ) {
			logger.error( "Server start failure.", ohohChongo );
		}
	}


	@Override
	public void close() {
		spark.Spark.stop() ;
	}
}
