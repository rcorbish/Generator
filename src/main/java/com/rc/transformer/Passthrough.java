
package com.rc.transformer ;

import java.util.Date;
import java.util.StringJoiner;

public class Passthrough extends Transformer {
	
	private int numberOfLinesProcessed ;
	
	public Passthrough() {
		numberOfLinesProcessed = 0 ;
	}
	
    @Override
    public CharSequence[] convert( Object data[] ) {
    	numberOfLinesProcessed++ ;
    	StringJoiner sj = new StringJoiner( "," ) ;
    	for( Object o : data ) {
    		sj.add( o==null ? "" : o.toString() ) ;
    	}
        return new CharSequence[] { sj.toString() } ;
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