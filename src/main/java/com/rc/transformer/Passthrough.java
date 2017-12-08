
package com.rc.transformer ;

import java.util.Date;

import com.rc.sink.Sink;

public class Passthrough extends Transformer {
	
	private int numberOfLinesProcessed ;
	
	public Passthrough() {
		numberOfLinesProcessed = 0 ;
	}
	
    @Override
    public void process( Object data[] ) {
    	numberOfLinesProcessed++ ;
        sink.consume( data ) ;
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