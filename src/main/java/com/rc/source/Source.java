
package com.rc.source ;

import java.io.IOException;
import java.util.stream.Stream;

@FunctionalInterface
public interface Source {
    
    public Stream<Object[]> get() throws IOException ;

}