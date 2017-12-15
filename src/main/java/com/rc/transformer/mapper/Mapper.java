
package com.rc.transformer.mapper ;

/**
 * This class converts an input set of columns (e.g. from DB or file )
 * into one output line
 */
@FunctionalInterface
public interface Mapper {

    public CharSequence process( Object data[] ) ;
    
}