
package com.rc.source ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

public class FileSource implements Source {

	final private Path path ;
	final private String separator ;
	final private String columnNames[] ;

	public FileSource( Path path ) throws IOException {
		this( path, "\\," ) ;
	}

	public FileSource( Path path, String separator ) throws IOException {
		this.path = path ;
		this.separator = separator ;
		Optional<String[]> tmp =Files.lines( path ).limit(1).map( s -> s.split( separator ) ).findFirst() ;
		columnNames = tmp.orElse( null ) ; 
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