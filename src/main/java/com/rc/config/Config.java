
package com.rc.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.Processor;
import com.rc.sink.FileSink;
import com.rc.sink.Sink;
import com.rc.source.FileSource;
import com.rc.source.Source;
import com.rc.transformer.Passthrough;
import com.rc.transformer.Transformer;
import com.rc.trigger.TimeTrigger;
import com.rc.trigger.Trigger;

public class Config {

    final static Logger log = LoggerFactory.getLogger(Config.class);
    final Path configFile;

    public Config(Path configFile) {
        this.configFile = configFile;
    }

    public Processor createProcessor() throws IOException, ParseException {
        log.info("Creating processor - {}", configFile.getFileName());

        parseConfigFile() ;

        Source source = new FileSource(Paths.get("/etc/passwd"), ":");
        Trigger trigger = new TimeTrigger(20, 0, TimeZone.getDefault(), Calendar.SUNDAY);

        Processor rc = new Processor(configFile.getFileName().toString(), source, trigger);

        Sink sink = new FileSink(Paths.get("/tmp/xxx"));
        Transformer transformer = new Passthrough(sink);

        rc.addTransformer(transformer);
        return rc;
    }

    public Config parseConfigFile() throws ParseException {
        Config rc = null ;

        String section = null ;
        Map<String,String> values = new LinkedHashMap<>() ;
        
        log.info("Parsing config file {}", configFile ) ;

        try( BufferedReader rdr = Files.newBufferedReader( configFile ) ) {
            // Gson gson = new GsonBuilder().setLenient().create() ;
            // rc = gson.fromJson( rdr, Config.class ) ;
            int lineNum = 1 ;
            String line = "" ;

            for( String s=rdr.readLine() ; s!=null ; s=rdr.readLine(), lineNum++ ) {
                s = s.trim() ;
                if( s.length()==0 || s.charAt(0) == '#' ) continue ;
                if( s.charAt(0) == '[' ) {
                    int ix = s.indexOf(']') ;
                    if( ix < 0 ) {
                        throw new ParseException( "Bad section formatting", lineNum ) ;
                    }
                    processSection( section, values ) ;
                    values.clear();
                    
                    section = s.substring(1, s.indexOf(']') ).trim().toLowerCase() ;
                } else if( s.charAt( s.length()-1) == '\\' ) {
                    line += s ;
                } else {
                    line += s ;
                    int ix = 0 ;
                    while( ix<line.length() && 
                            !Character.isWhitespace(line.charAt(ix)) ) {
                        ix++ ;
                    }
                    String key = line.substring(0,ix) ;
                    String value = line.substring(ix).trim() ;
                    values.put( key, value ) ;
                    line = "" ;
                }
            } 
            processSection( section, values ) ;
        } catch (IOException iex) {
            rc = null;
        }
        return rc;
    }

    public void processSection( String sectionName, Map<String,String> values ) {
        if( sectionName !=null ) {
            log.info( "Processed {} using {}", sectionName, values ) ;
        }
    }
}
