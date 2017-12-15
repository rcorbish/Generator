

package com.rc.transformer ;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.config.Config;
import com.rc.transformer.mapper.EmptyMapper;
import com.rc.transformer.mapper.Mapper;
import com.rc.transformer.mapper.PassthroughMapper;

public class MappingTransformer extends Transformer {

	final private static Logger log = LoggerFactory.getLogger(MappingTransformer.class);
	final private String columnNames[] ;

	final private ScriptEngineManager sem ;
	final private ScriptEngine engine ;
	final private Bindings bindings ;
	int rowNum ;
	long started ;

	final List<List<Mapper>> mapperSet ;

	public MappingTransformer( Map<String,LinkedHashMap<String,String>> columnsMapping, String inputColumns[] ) throws Exception {

		sem = new ScriptEngineManager() ;
		engine = sem.getEngineByName("js") ;
		bindings = engine.createBindings() ;
		engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE );

		// Get a list of all the output column names => columnNames
		LinkedHashMap<String,String> colNameMap = columnsMapping.get( Config.COLUMN_NAMES ) ;
		columnNames = new String[ colNameMap.size() ] ;
		colNameMap.keySet().toArray( columnNames ) ;
		
		// This holds the mappings for each section - keyed on section name
		this.mapperSet = new ArrayList<List<Mapper>>() ;

		// Find some defaultMappers = i.e. read the input column into the output (if exists else empty )
		List<Mapper> defaultMappers = new ArrayList<>() ;
		for( String key : colNameMap.keySet() ) {
			String from = colNameMap.get( key ) ;
			int ix = getInputColumnIndex( inputColumns, from ) ;
			Mapper mapper = ix<0 ? new EmptyMapper() : new PassthroughMapper( ix ) ;
			defaultMappers.add( mapper ) ;
		}

		
		Pattern colRef = Pattern.compile( "\\$\\{([a-zA-Z0-9\\s]+)}" ) ;

		// For all the subsections ...
		for( String section : columnsMapping.keySet() ) {
			// keep these 2 special data sets out of mapping
			if( section.equals( Config.GLOBAL_TRANSFORM ) || section.equals( Config.COLUMN_NAMES) ) {
				continue ;
			}
			LinkedHashMap<String, String> sectionDetails = columnsMapping.get(section) ;
			// The column mappers for this section
			// will default to the simple passthrough 
			List<Mapper> mappers = new ArrayList<>( defaultMappers ) ;
			
			// For each column that has custom behaviour ...
			for( String col : sectionDetails.keySet() ) {
				int colIndex = getInputColumnIndex(columnNames, col ) ;

				// What is the custom behaviour
				String val = sectionDetails.get( col ) ;
				// If empty - we'll override to an empty mapper
				if( val.length() == 0 ) {
					mappers.set( colIndex, new EmptyMapper() ) ;
					continue ;
				}
				
				// Is there a #{xxx} in there - then replace it with the column xxx
				// by replacing that text with data[n]  ( n = appropriate input column index )  
				Matcher m = colRef.matcher(val) ;
				while( m.find() ) {
                    String lookedUpColumn = m.group(1) ;
					String inputColumn = colNameMap.getOrDefault(lookedUpColumn, "" ) ;
					
					int inputColIndex = -1 ;
					int ix = 0 ;
					for( String colName : inputColumns ) {
						if( colName.equals( inputColumn ) ) {
							inputColIndex = ix ;
							break ;
						}
						ix++ ;
					}
                    log.debug( "Matched {} = {}[{}]", lookedUpColumn, (Object[])inputColumns, inputColIndex) ;
					if( inputColIndex>=0 ) {
						val = m.replaceFirst( "data[" + ix + "]" ) ;
					}
				}
				
				// Create a new mapper based on the javascript in the value
				String script = "var obj={ process : function( data ) { return String( " + val + ") ; } }" ;
				engine.eval( script, bindings ) ;
				Object obj = engine.get( "obj" ) ;
				Mapper mapper = ((Invocable)engine).getInterface( obj, Mapper.class ) ;
				
				// And override the default mapping ...  
				mappers.set( colIndex, mapper ) ;				
			}
			
			// Set this mapper in the list of output mappers
			this.mapperSet.add( mappers ) ;
		}
	}

	// Find the index of a string in an (unsorted) array
	private int getInputColumnIndex( String columns[], String column ) {
		int ix = 0 ;
		for( String colName : columns ) {
			if( colName.equals( column ) ) {
				return ix ;
			}
			ix++ ;
		}
		return -1 ;
	}
	
	// call each mapper defined by the constructor
    // and send the result to the consumer
    @Override
	public CharSequence[] convert( Object data[] ) {
		rowNum++ ;
        bindings.put( "row_num", rowNum ) ;
        CharSequence rc[] = new CharSequence[ mapperSet.size() ] ;

        int csIndex = 0 ;
		for( List<Mapper> mappers : mapperSet ) {
			StringBuilder sb = new StringBuilder("\"") ;
			for( Mapper mapper : mappers ) {
				CharSequence colValue = mapper.process( data ) ;
				sb.append( "|\"").append( colValue ).append( "\"" ) ;
			}
            rc[csIndex] = sb ;
            csIndex++ ;
        }
        return rc ;
	}

	public void preProcess() {
		sink.header( "" );
		rowNum = 0 ;
		started = System.currentTimeMillis() ;
	}
	public void postProcess() {
		long delta = System.currentTimeMillis() - started ;
		log.info( "Processed {} lines in {}mS [ {} lines/sec ]", rowNum, delta, (rowNum * 1000) / delta ) ;
		sink.footer( "" );
	}


}