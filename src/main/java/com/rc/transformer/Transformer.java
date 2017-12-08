
package com.rc.transformer ;

import java.util.stream.Stream;

abstract public class Transformer {

    public final void process( Stream<Object[]> data ) {
    	preProcess() ;
    	data.forEach( this::process ) ;
    	postProcess() ;
    }

    abstract public void process( Object data[] ) ;
    abstract public void preProcess() ;
    abstract public void postProcess() ;
    
}