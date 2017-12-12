
package com.rc.source ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.sink.FileSink;

public class FileSource implements Source {

	final static Logger log = LoggerFactory.getLogger( FileSink.class ) ;

	final private Path path ;
	final private String separator ;
	final private boolean headers ;
	String columns[] ;

	public FileSource( Path path ) throws IOException {
		this( path, true ) ;
	}

	public FileSource( Path path, boolean headers ) throws IOException {
		this( path, "\\,", headers ) ;
	}

	public FileSource( Path path, String sep ) throws IOException {
		this( path, sep, true ) ;
	}

	public FileSource( Path path, String separator, boolean headers ) throws IOException {
		this.path = path ;
		this.separator = separator ;
		this.headers = headers ;
		if( headers ) {
			List<String[]> hdrs = Files.lines( path ).limit(1).map( s -> s.split( separator ) ).collect( Collectors.toList() ) ;
			if( !hdrs.isEmpty() ) {
				columns = hdrs.get(0) ;
			}
		}
	}

	@Override
	public Stream<Object[]> get() throws IOException {
		if( headers ) {
			return Files.lines( path ).skip(1).map( s -> s.split( separator ) ) ;
		} else {
			return Files.lines( path ).map( s -> s.split( separator ) ) ;
		}
	}

	@Override
	public String[] getColumns() {
		return columns ;
	}


	static Path MostRecentFile( Path a, Path b ) {
		try { 
			return Files.getLastModifiedTime(a).
					compareTo( Files.getLastModifiedTime(b) ) > 0 ? a : b  ;
		} catch( IOException ignore ) {

		}
		return null ;
	}

	public static Path getFileNameFromFile( String fileName ) throws IOException {
		Path input = Paths.get( fileName ) ;		
		Files.createDirectories( input.getParent() ) ;
		if( Files.isDirectory(input) ) {
			throw new IOException( "File " + fileName + " exists as directory" ) ;
		}
		return input ;
	}

	public static Path getFileNameFromDir( String fileName ) throws IOException {
		Path input = Paths.get( fileName ) ;		
		Optional<Path> p = Files.list( input ).reduce( (a,b) -> FileSource.MostRecentFile(a,b) ) ;
		log.info( "Found most recent file: {}", p.isPresent()?p.get():"-" ) ;
		Path rc = p.isPresent() ? p.get() : null ;
		log.info( "Opening {} in input directory {}", rc, input ) ;
		return rc ;
	}

}