
package com.rc.sink ;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileSink implements Sink {

    final static Logger log = LoggerFactory.getLogger( FileSink.class ) ;
    
	private BufferedWriter out ;

	final private Path output ;

	public FileSink( Path outputDirectory ) throws IOException {
		this.output = outputDirectory ;
	}

	static Path MostRecentFile( Path a, Path b ) {
		try { 
			return Files.getLastModifiedTime(a).
				compareTo( Files.getLastModifiedTime(b) ) > 0 ? a : b  ;
		} catch( IOException ignore ) {

		}
		return null ;
	}

	@Override
	public void start() {
    	try {	
    		out = Files.newBufferedWriter( output ) ;
		} catch (IOException e) {
			log.error( "Failed to open () for writing.", output, e ) ;
		}
	}
	
	@Override
	public void finish() {
    	try {	
	    	out.flush(); 
	    	out.close();
	    	out = null ;
		} catch (IOException e) {
			log.error( "Failed to close ().", output, e ) ;
		}
	}
	
	
	@Override
    public void header( CharSequence data ) {
    	try {	
			out.append( data ) ;
			out.newLine() ;
		} catch (IOException e) {
			log.error( "Failed to write header to ()", output, e ) ;
		}
    }
    
	@Override
    public void consume( CharSequence data ) {
    	try {
			out.append( data ) ;
			out.newLine() ;
		} catch (IOException e) {
			log.error( "Failed to write data to ()", output, e  ) ;
		}
    }
    
	@Override
    public void footer( CharSequence data ) {
    	try {
			out.append( data ) ;
		} catch (IOException e) {
			log.error( "Failed to write footer to ()", output, e ) ;
		}
    }


	public static Path getFileNameFromFile( String fileName ) throws IOException {
		Path output = Paths.get( fileName ) ;		
		Files.createDirectories( output.getParent() ) ;
		if( Files.isDirectory(output) ) {
			throw new IOException( "File " + fileName + " exists as directory" ) ;
		}
		return output ;
	}

	public static Path getFileNameFromDir( String fileName ) throws IOException {
		Path output = Paths.get( fileName ) ;		
		Files.createDirectories( output ) ;
		Optional<Path> p = Files.list( output ).reduce( (a,b) -> FileSink.MostRecentFile(a,b) ) ;
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
		Path rc = output.resolve( String.format( "%04d", sequenceNum) ) ;
		log.info( "Creating new {} in output directory {}", rc.getFileName(), output ) ;
		return rc ;
	}

}