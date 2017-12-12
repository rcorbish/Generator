
package com.rc.transformer ;

import java.util.stream.Stream;

import com.rc.sink.Sink;

abstract public class Transformer {

    public Sink sink ;

    public final void process( Stream<Object[]> data ) {
    	sink.start() ;
    	preProcess() ;
    	data.forEach( this::process ) ;
    	postProcess() ;
    	sink.finish() ;
    }

    abstract public void process( Object data[] ) ;
    abstract public void preProcess() ;
    abstract public void postProcess() ;
    
}