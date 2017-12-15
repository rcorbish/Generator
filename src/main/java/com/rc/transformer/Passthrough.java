
package com.rc.transformer ;

import java.util.Date;
import java.util.StringJoiner;

import com.rc.sink.Sink;

public class Passthrough extends Transformer {
	
	private int numberOfLinesProcessed ;
	
	public Passthrough() {
		numberOfLinesProcessed = 0 ;
	}
	
    @Override
    public void process( Object data[] ) {
    	numberOfLinesProcessed++ ;
    	StringJoiner sj = new StringJoiner( "," ) ;
    	for( Object o : data ) {
    		sj.add( o==null ? "" : o.toString() ) ;
    	}
        sink.consume( sj.toString() ) ;
    }

	@Override
	public void  preProcess() {
		sink.header( "Created on: " + new Date() + "." ) ;
	}

	@Override
	public void postProcess() {
		sink.footer( "Processed: " + numberOfLinesProcessed + " lines." ) ;
	}

}