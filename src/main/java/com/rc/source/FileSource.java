
package com.rc.source ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a class to provide lines from a file.
 * The different constructors provide control of loading headers
 * and the delimiter
 */
public class FileSource implements Source {

	final private static Logger log = LoggerFactory.getLogger(FileSource.class);
	
	final private Path path ;
	final private String separator ;
	final private String columnNames[] ;

	public FileSource( Path path ) throws IOException {
		this( path, "\\," ) ;
	}

	public FileSource( Path path, String columnNames[] ) throws IOException {
		this( path, "\\,", columnNames) ;
	}

	public FileSource( Path path, String separator ) throws IOException {
		this( path, separator, null ) ;
	}

	public FileSource( Path path, String separator, String columnNames[] ) throws IOException {
		this.path = path ;
		this.separator = separator ;
		if( columnNames == null ) {
			Optional<String[]> tmp =Files.lines( path ).limit(1).map( s -> s.split( separator ) ).findFirst() ;
			this.columnNames = tmp.orElse( null ) ; 
		} else {
			this.columnNames = columnNames ;
		}
	}

	@Override
	public Stream<Object[]> get() throws IOException {
		return Files.lines( path ).map( s -> s.split( separator ) ) ;
	}

	@Override
	public String []columnNames() {
		return columnNames ;
	}
}