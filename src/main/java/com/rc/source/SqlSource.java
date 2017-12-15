
package com.rc.source ;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqlSource implements Source {
    
    final static Logger log = LoggerFactory.getLogger(SqlSource.class);
    
    private final Connection connection ;
    private final PreparedStatement stmt ;
    private final ResultSet rs ;
    private final String columnNames[] ;
    private final int numColumns ;
    
    public SqlSource( String connectionString, 
                        String userName, 
                        String password, 
                        String sql, 
                        Object ... args ) throws SQLException {
        connection = DriverManager.getConnection(connectionString, userName, password ) ;
        stmt  = connection.prepareStatement( sql ) ;
        for( int i=0 ; i<args.length ; i++ ) {
            stmt.setObject( i+1, args[i] ) ;
        }
        rs = stmt.executeQuery() ;
        ResultSetMetaData rsmd = rs.getMetaData() ;
        numColumns = rsmd.getColumnCount() ;
        columnNames = new String[ numColumns ] ;
        for( int i=0 ; i<columnNames.length ; i++ ) {
        	columnNames[i] = rsmd.getColumnLabel( i+1 ) ;
        }
    }

    @Override
    public Stream<Object[]> get() {
        Stream<Object[]> rc = null ;
        try {
            ResultsetSpliterator rss = new ResultsetSpliterator() ;
            rc = StreamSupport.stream( rss, false );
        } catch( SQLException sex ) {
            log.error( "Error geting SQL data", sex ) ;
        }
        return rc ;
    }

    @Override
    public String []columnNames() {
    	return columnNames ;
    }
    
    public void close() {
        try {
            rs.close(); 
            stmt.close();
            connection.close();
        } catch( SQLException sex ) {
            log.error( "Error closing SQL source", sex ) ;
        }
    }

    class ResultsetSpliterator extends AbstractSpliterator<Object[]> {
        
        ResultsetSpliterator() throws SQLException {
            super( 0L, IMMUTABLE ) ;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Object[]> consumer) {
            boolean rc = false ;
            try {
                Object data[] = new Object[ numColumns ] ;
                for( int i=0 ; i<numColumns ; i++ ) {
                    data[i] = rs.getObject( i+1 ) ;                
                }
                consumer.accept( data ) ;
                rc = true ;
            } catch( SQLException sex ) {
                log.error( "Error reading DB", sex ) ;
            }
            return rc ;
        }
    }    
}