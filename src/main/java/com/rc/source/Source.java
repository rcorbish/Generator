
package com.rc.source ;

import java.io.IOException;
import java.util.stream.Stream;

public interface Source {
    
    public Stream<Object[]> get() throws IOException ;
    public String [] getColumns() ; 
}