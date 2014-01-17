package at.ac.ait.dme.video_batch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

/**
 * MediaFramework hides a concrete media framework in use to process media data.
 * It is solely accessed by VideoBatch.
 * 
 * @author Matthias Rella
 */


// TODO: find out which types of media can be processed. It may be that streamed types don't work (no keyframes etc.)

public interface MediaFramework {

    /**
     * Initialize the framework with a given media container file. 
     * @param file full URI location of media container file
     * @throws FileNotFoundException
     */
    public abstract void init( URI file ) throws RuntimeException;

    /**
     * Generates a sorted TreeMap of byte-level index positions mapping to presentation timestamps. 
     * Can be done for all streams as a whole or for the video stream only. 
     * 
     * @param videoOnly whether only the video stream shall be regarded
     * @return TreeMap<Long,Long> a sorted map of index positions to presentation timestamps
     * @throws IOException in case of no index entries being found 
     * 
     */
    public abstract TreeMap<Long, Long> getIndices( boolean videoOnly ) throws IOException;

    /**
     * Gets the average size of a Group-Of-Pictures in the video stream of the media container.
     * @return long average size value
     * @throws IOException in case of no index entries being found. Return total filesize if no GOPs are found. 
     */
    public abstract long getAvgGOPSize();

    /**
     * Gets the average number of pictures in a Group-Of-Pictures in the video stream of the media container.
     * @return int average number of pictures. Returns total number of frames if no GOPs are found.
     */
    public abstract long getAvgGOPLength();

    public abstract String getCodecPackageName();

}
