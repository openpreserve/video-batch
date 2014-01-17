package at.ac.ait.dme.video_batch;

/**
 * Structure to denote audio/video encoding settings.
 */

public abstract class AVEncoding {
    
    public abstract void setProperty( String key, Object value );
    public abstract Object getProperty( String key );
        
}
