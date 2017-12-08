
package com.rc.sink ;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileSink implements Sink {

    final static Logger log = LoggerFactory.getLogger( FileSink.class ) ;
    
	private BufferedWriter out ;

	final private Path outputDirectory ;
    public FileSink( Path outputDirectory ) throws IOException {
		this.outputDirectory = outputDirectory ;
		Files.createDirectories( outputDirectory ) ;
	}

	static Path MostRecentFile( Path a, Path b ) {
		try { 
			return Files.getLastModifiedTime(a).
				compareTo( Files.getLastModifiedTime(b) ) > 0 ? a : b  ;
		} catch( IOException ignore ) {

		}
		return null ;
	}

	private Path getFileName() throws IOException {
		Optional<Path> p = Files.list( outputDirectory ).reduce( (a,b) -> FileSink.MostRecentFile(a,b) ) ;
		log.info( "Found most recent file: {}", p.isPresent()?p.get():"-" ) ;
		int sequenceNum = 0 ;
		if( p.isPresent() ) {
			try {
				String fn = p.get().getFileName().toString() ;
				int ix = fn.lastIndexOf('.') ;
				sequenceNum = Integer.parseInt( fn.substring(ix+1) ) ;
			} catch( Throwable t ) {
			}
		}
		sequenceNum++ ;
		return outputDirectory.resolve( String.format( "%04d", sequenceNum) ) ;
	}

    public void header( String data ) {
    	try {	
            out = Files.newBufferedWriter( getFileName() ) ;
			out.append( data ) ;
			out.newLine() ;
		} catch (IOException e) {
			log.error( "Failed to write header to ()", outputDirectory, e ) ;
		}
    }
    
    public void consume( Object data[] ) {
    	try {
			out.append( 
				Stream.of( data )
				.map( e -> e==null ? "" : e.toString() )
				.collect( Collectors.joining( "','", "'", "'" ) ) 
			) ;
			out.newLine() ;
		} catch (IOException e) {
			log.error( "Failed to write data to ()", outputDirectory, e  ) ;
		}
    }
    
    public void footer( String data ) {
    	try {
			out.append( data ) ;
	    	out.flush(); 
	    	out.close();
	    	out = null ;
		} catch (IOException e) {
			log.error( "Failed to write footer to ()", outputDirectory, e ) ;
		}
    }
}