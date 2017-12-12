

package com.rc.transformer ;

import java.util.LinkedHashMap;
import java.util.StringJoiner;

public class MappingTransformer extends Transformer {

    final String[] columnNames ;
    final int[] dataIndices ;
    int numLinesProcessed ;
    
    public MappingTransformer( LinkedHashMap<String,String> columnsMapping, String inputColumns[] ) {
        this.columnNames = new String[ columnsMapping.size() ] ;
        this.dataIndices = new int[ columnsMapping.size() ] ;
        
        columnsMapping.keySet().toArray( this.columnNames ) ;
        
        for( int i=0 ; i<columnsMapping.size() ; i++ ) {
        	dataIndices[i] = -1 ;
        	String colName = columnsMapping.get( columnNames[i] ) ;
        	for( int j=0 ; j<inputColumns.length ; j++ ) {
        		if( inputColumns[j].equals(colName) ) dataIndices[i] = j ;
        	}
        }
    }

    
    public void process( Object data[] ) {
    	numLinesProcessed++ ;
    	
    	StringJoiner sj = new StringJoiner( "," ) ;
    	for( int dataIndex : dataIndices ) {
    		if( dataIndex < 0 ) {
    			sj.add( "" ) ;
    		} else {
    			sj.add( data[dataIndex].toString() ) ;
    		}
    	}
    	sink.consume( sj.toString() );
    }
    
    public void preProcess() {
    	numLinesProcessed = 0 ;
    	
    	StringJoiner sj = new StringJoiner( "," ) ;
    	for( String col : columnNames ) {
    		sj.add( col ) ;
    	}
    	sink.consume( sj.toString() );
    }
    
    public void postProcess() {
    	String s = "Printed " + numLinesProcessed + " lines" ;
    	sink.consume( s );
    }
    
}