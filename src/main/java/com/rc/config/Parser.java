package com.rc.config ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class Parser {

    public List<Config> parse( Path directory, Path scriptDir ) throws IOException {
        return Files.list( directory )
            .filter( this::isConfigFile )
            .map( e -> parseConfig(e,scriptDir) )
            .collect( Collectors.toList() )
        ;
    }

    private Config parseConfig( Path configFile, Path scriptDir ) {
        Config rc = new Config( configFile, scriptDir ) ;
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