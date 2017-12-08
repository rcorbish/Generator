
package com.rc.source ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileSource implements Source {
    
    final private Path path ;
    final private String separator ;

    public FileSource( Path path ) {
        this( path, "\\," ) ;
    }

    public FileSource( Path path, String separator ) {
            this.path = path ;
            this.separator = separator ;
    }
    
    @Override
    public Stream<Object[]> get() throws IOException {
        return Files.lines( path ).map( s -> s.split( separator ) ) ;
    }

}