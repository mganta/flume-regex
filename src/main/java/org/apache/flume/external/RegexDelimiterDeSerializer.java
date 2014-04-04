/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.external;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A deserializer that parses an "event" from a file. What defines
 * an "event" is where blocks of text are split on via the configurable
 * "eventEndRegex" context property. This deserializers treats a block
 * as starting from the current position of its InputStream and reads
 * up until it encounters the first match of "eventEndRegex". Whether or 
 * not the "eventEndRegex" is included in the matched block of data 
 * is controlled by the "includeEventEndRegex" parameter (default to true)
 * 
 * includeEventEndRegex is generally set to TRUE if you are using a delimiter
 * that is at the end of your event and a relevant part of your event.
 * 
 * includeEventEndRegex is generally set to FALSE if you are using a delimiter
 * that happens to be mark "start" of events, and you don't want it returned
 * as part of the event it just parsed (the event preceding it). 
 * 
 * IMPORTANT! The "newline" character ("\n") CANNOT be part of the regex pattern
 * to split blocks
 * 
 * @author bitsofinfo.g [at] gmail.com
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegexDelimiterDeSerializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger
      (LineDeserializer.class);

  private final ResettableInputStream in;
  private final Charset outputCharset;
  private volatile boolean isOpen;
  private final String eventEndRegex;
  private final Pattern eventEndPattern;
  private boolean includeEventEndRegex = false;


  public static final String INCLUDE_EVENT_END_REGEX_KEY = "includeEventEndRegex";
  public static final String INCLUDE_EVENT_END_REGEX_DFLT = "false";
  
  public static final String EVENT_END_REGEX_KEY = "eventEndRegex";
  public static final String EVENT_END_REGEX_DFLT = "\n";
  
  public static final String OUT_CHARSET_KEY = "outputCharset";
  public static final String CHARSET_DFLT = "UTF-8";


  RegexDelimiterDeSerializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.outputCharset = Charset.forName(
        context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
    this.eventEndRegex = context.getString(EVENT_END_REGEX_KEY, EVENT_END_REGEX_DFLT);
    this.includeEventEndRegex = Boolean.valueOf(context.getString(INCLUDE_EVENT_END_REGEX_KEY, 
    									INCLUDE_EVENT_END_REGEX_DFLT));
    this.isOpen = true;
    
    this.eventEndPattern = Pattern.compile(eventEndRegex);
  }

  /**
   * Reads the next block from a file and returns an event
   * 
   * @return Event containing parsed block
   * @throws IOException
   */
  
  public Event readEvent() throws IOException {
    ensureOpen();
    String block = readNextBlock();
    if (block == null) {
      return null;
    } else {
      return EventBuilder.withBody(block, outputCharset);
    }
  }

  /**
   * Batch block read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read blocks
   * @throws IOException
   */
 
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }


  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }


  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  // TODO: consider not returning a final character that is a high surrogate
  // when truncating
  private String readNextBlock() throws IOException {
    StringBuilder blockBuffer = new StringBuilder();
    StringBuilder lineBuffer = new StringBuilder();
    
    boolean blockFound = false;
    boolean atBOB = true; // at beginning of block?
    boolean atEOF = false; // at end of file?
    
    while(!blockFound) {
    
		// read in next line
    	lineBuffer.delete(0, lineBuffer.length());
	    int c;
	    while ((c = in.readChar()) != -1) {
	 
	      lineBuffer.append((char)c);
	      
	      if (c == '\n') {
	    	  break;
	      }
	    }
	    
	    if (c == -1) {
	    	atEOF = true;
	    }
	    
	    if (atEOF && lineBuffer.length() == 0) {
	    	return null;
	    	
	    } else if (atEOF) {
	    	blockBuffer.append(lineBuffer.toString());
	    	break;
	    }
	    
	    String currentLine = lineBuffer.toString();
	    Matcher matcher = eventEndPattern.matcher(currentLine);
	    
	    // if we find a match AND do not include the eventEndRegex and started at BOB...
	    // then we can capture everything up to this point, otherwise we may have
	    // matched the beginning of a record at the start of the block 
	    // (where regex is using start of records as an "ending" marker for the previous record)
	    if (matcher.find() && !(atBOB && !includeEventEndRegex)) {
	    	
	    	blockFound = true;
	    	int matchStart = matcher.start();
	    	int matchEnd = matcher.end();
	    	
	    	// capture the current line up to the END of the match
	    	// by default, it INCLUDES the matched text
	    	int captureLineUpToPosition = matchEnd;
	    	if (!includeEventEndRegex) {
	    		captureLineUpToPosition = matchStart;
	    	}
	    	
	    	// append the relevant portion of the current line 
	    	blockBuffer.append(currentLine.substring(0,captureLineUpToPosition));
	    	
	    	long currLineLength = currentLine.length();
	    	long charsToReadBackTo = (currLineLength - captureLineUpToPosition);
	    	
	    	long seekBackTo = in.tell() - charsToReadBackTo;
	    	in.seek(seekBackTo);
	    	
	    } else {
	    	blockBuffer.append(currentLine);
	    	
	    	// if we started at BOB, we definately are no longer there
	    	if (atBOB) {
	    		atBOB = false;
	    	}
	    }
	}
    
    return blockBuffer.toString();
   
  }

  public static class Builder implements EventDeserializer.Builder {

    public EventDeserializer build(Context context, ResettableInputStream in) {
      return new RegexDelimiterDeSerializer(context, in);
    }

  }

}
