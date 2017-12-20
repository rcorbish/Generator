
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

	static Path LargestSequencedFile( Path a, Path b ) {
		int sequenceNumA = -1 ;
		int sequenceNumB = -1 ;
		try {
			String fnA = a.getFileName().toString() ;
			int ixA = fnA.lastIndexOf('.') ;
			sequenceNumA = Integer.parseInt( fnA.substring(ixA+1) ) ;
		} catch( Exception e ) {
			log.warn( "Error checking most recent file - {}", a, e ) ;
		}
		try {
			String fnB = b.getFileName().toString() ;
			int ixB = fnB.lastIndexOf('.') ;
			sequenceNumB = Integer.parseInt( fnB.substring(ixB+1) ) ;
		} catch( Exception e ) {
			log.warn( "Error checking most recent file - {}", b, e ) ;
		}
	
		return sequenceNumA > sequenceNumB ? a : b ;
	}


    @Override
    public void header( CharSequence data ) {
    	try {	
    		out = Files.newBufferedWriter( output ) ;
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
			out.flush();
			out.close();
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

	/**
	 * Finds a filename using a pattern of /d1/d2/f1.ext.0001
	 * The most file with largest sequence  is used - NOT the most recently edited
	 */
	public static Path getFileNameFromDir( String fileName ) throws IOException {
		Path output = Paths.get( fileName ) ;		
		Files.createDirectories( output ) ;
		Optional<Path> p = Files.list( output ).reduce( (a,b) -> FileSink.LargestSequencedFile(a,b) ) ;
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