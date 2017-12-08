package com.rc.config ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class Parser {

    public List<Config> parse( Path directory ) throws IOException {
        return Files.list( directory )
            .filter( this::isConfigFile )
            .map( this::parseConfig )
            .collect( Collectors.toList() )
        ;
    }

    private Config parseConfig( Path configFile ) {
        Config rc = new Config( configFile ) ;
        return rc ;
    }

    private boolean isConfigFile( Path path ) {
        boolean rc = true ;
        try {
            rc = Files.isRegularFile( path ) &&
                Files.isReadable( path ) &&
                !Files.isHidden( path ) 
                ;
        } catch( IOException ioe ) {
            rc = false ;
        }
        return rc ;
    }
}