package at.ac.ait.dme.video_batch.xuggle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.naming.CannotProceedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IIndexEntry;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.io.URLProtocolManager;

/**
 * Provides methods to prepare an input video for processing. Uses Xuggler.
 *
 * @deprecated
 * @author Matthias Rella, DME-AIT
 */
public final class XugglerVideoAnalyzer {
    
    private static final Log LOG = LogFactory.getLog(XugglerVideoAnalyzer.class);
    private static URLProtocolManager mgr = URLProtocolManager.getManager();

    
    public static void addProtocol( String protocol ) {
        if( protocol.equals( "hdfs" )) 
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory() );
    }
    /*
     * Creates a Container from a local file.
     * @param String input - video file name
     * @return IContainer container
     */
    public static IContainer makeContainer( String input )  {
        
        IContainer container = IContainer.make();
        
        LOG.debug( input );
        
	    //if (container.open(dis, null ) < 0)
        int r = container.open(input, IContainer.Type.READ, null);
        if( r < 0)
        {
          IError error = IError.make( r );
	      throw new IllegalArgumentException("could not open container from " + input + ", error = " + error.getDescription() );
        }
        
        return container;
        
    }
    
    public static int getMeanGOPLength(IContainer container) throws CannotProceedException, IOException {
	    int numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new IOException( "No streams found in container." );
	    
	    List<IIndexEntry> index_list = null;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list = stream.getIndexEntries();
	    }
	    
	    if( index_list == null || index_list.size() == 0)
	        throw new CannotProceedException( "No index entries found!" );
	    
	    int k = 0;
	    for( int i = 0; i < index_list.size(); i++ )
	       if ( index_list.get(i).isKeyFrame() ) k++; 
	    
        return index_list.size() / k;
    }
    
    /**
     * Calculates average size of a group of pictures (GOP) in bytes of the video stream of the given container.
     * @param container
     * @return
     * @throws IOException
     * @throws CannotProceedException
     */
    public static long getMeanGOPSize( IContainer container ) throws IOException, CannotProceedException {
        
	    int numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new IOException( "No streams found in container." );
	    
	    List<IIndexEntry> index_list = null;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list = stream.getIndexEntries();
	    }
	    
	    if( index_list == null || index_list.size() == 0)
	        throw new CannotProceedException( "No index entries found!" );
	    
	    
        BigInteger avg_gop = new BigInteger("0");
        
        // move on to first keyframe
        int i = 0;
        while( i < index_list.size() && !index_list.get(i).isKeyFrame() ) i++;
        
        int k = 0; // count of keyframes i.e. GOPs
        
	    while( i < index_list.size() )
	    {
            k++;
	        IIndexEntry entry = index_list.get(i);
	        
            BigInteger size = new BigInteger( entry.getSize() + "" );
            LOG.debug( "keyframe at " + i);
            i++;
            // as long as there are no key frame, sum up frame sizes
            while( i < index_list.size() && !( entry = index_list.get(i) ).isKeyFrame() ) {
                size = size.add( new BigInteger( entry.getSize() + "" ));
                LOG.debug( "non-keyframe at " + i);
                i++;
	        }
            // when key frame is reached, add gop size
            avg_gop = avg_gop.add( size );
	    }
	    
	    if( k > 0 )
	        avg_gop = avg_gop.divide( new BigInteger( k + ""));
	    
	    return avg_gop.longValue();
    }
    
    /**
     * Calculates average frame size in bytes of the video stream of the given container.
     * @param container
     * @return
     * @throws IOException
     * @throws CannotProceedException
     */
    public static int getMeanVideoFrameSize( IContainer container ) throws IOException, CannotProceedException {
        
	    int numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new IOException( "No streams found in container." );
	    
	    List<IIndexEntry> index_list = null;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list = stream.getIndexEntries();
	    }
	    
	    if( index_list == null || index_list.size() == 0)
	        throw new CannotProceedException( "No index entries found!" );
	    
	    BigInteger avg_size = new BigInteger("0");
	    
	    for( int i = 0; i < index_list.size(); i++ )
	        avg_size = avg_size.add( new BigInteger( index_list.get(i).getSize() + "") );
	    
	    avg_size = avg_size.divide( new BigInteger( index_list.size() + "" ));
		
        return avg_size.intValue();
    }
    
    /**
     * Generates a List of IndexEntries of all streams or of video stream only out of a container.
     * @param IContainer container 
     * @param onlyVideo - whether IndexEntries of video stream only should be used
     * @return
     * @throws Exception 
     * 
     * @Deprecated
     */
    private static List<Long> getIndexEntries(IContainer container, boolean onlyVideo ) throws Exception {
	    
	    // query how many streams the call to open found
	    int numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new Exception( "No streams found in container." );
	    
	    LOG.debug( "numStreams: " + numStreams );
	    
	    if( onlyVideo ) numStreams = 1;
	    
	    // all_indices will contain all byte positions of index entries of all streams in sequential order
	    ArrayList<Long> all_indices = new ArrayList<Long>();
	    
	    
	    // get lists of index entries of the streams
	    List<IIndexEntry> index_list[] = new List[numStreams];
	    boolean[] get_keyframes = new boolean[numStreams];
	    int list_done = 0;
	    
	    int i = 0;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( onlyVideo && coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list[i] = stream.getIndexEntries();
	        if( index_list[i].size() == 0 ) list_done++;
	        
	        if( LOG.isDebugEnabled() ) {
		        String output = "Index List " + i + ": Size " + index_list[i].size() + " : ";
		        String list = ""; 
		        for( int k = 0; k < index_list[i].size(); k++ )
		        {
		            output += index_list[i].get(k).getPosition() + ", ";
		            list += index_list[i].get(k).getTimeStamp() + ":" + index_list[i].get(k).getPosition() + "\n";
		        }
		        File f = new File( System.getProperty("user.home" ) + "/indices" );
		        try {
                    FileOutputStream fos = new FileOutputStream( f );
                    fos.write( list.getBytes() );
                    fos.close();
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
		        
		        //LOG.debug( output );
		        
	        }
	        
	        // if the stream is a video stream get_keyframes flag is set
	        get_keyframes[i] = coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO;
	        LOG.debug( "get_keyframes : " + get_keyframes[i] );
	        i++;
	        
	        // video stream was found. if "onlyVideo" then don't look at any other streams
	        if( onlyVideo) break;
	    }
	    
	    int counter[] = new int[numStreams];
	    for( int j = 0; j < numStreams; j++ ) counter[j] = 0;
	    
	    // iterate over all streams and put the smallest (key-)index-entry of the current index-entries of the streams
	    // into all_indices-list. stops when all list have been parsed.
	    while( list_done < numStreams ) {
	        long min_pos = -1;
	        int list_nr = -1;
	        for( int j = 0; j < numStreams; j++ ) {
	            LOG.debug( "index_list["+j+"] is null ? " + ( index_list[j] == null ));
	            if( index_list[j].size() > counter[j] ) {
	                
	                // if get_keyframes is true for this stream move up to next keyframe
	                while( get_keyframes[j] && 
	                        counter[j] < index_list[j].size() && 
	                        !index_list[j].get(counter[j]).isKeyFrame() ) counter[j]++;
	                if( counter[j] >= index_list[j].size() ) {
	                    list_done++;
	                    continue;
	                }
	                                                                                        
	                // get index with smallest current_index-position 
	                IIndexEntry current_index = index_list[j].get(counter[j]);
	                if( min_pos == -1 || current_index.getPosition() < min_pos )
	                {
	                    min_pos = current_index.getPosition();
	                    list_nr = j;
	                }
	            }
	        }
	        
	        if( list_nr == -1 ) break;
            counter[list_nr]++;
	        if( index_list[list_nr].size() <= counter[list_nr] ) list_done++;
	        
	        all_indices.add( min_pos );
	    }
	    // end-of-file is also treated a index entry
	    all_indices.add( container.getFileSize() );
        return all_indices;
    }
    
    /**
     * Generates a sorted TreeMap of positions of IndexEntries to Presentation timestamps (frame numbers) 
     * of all streams or of video stream only out of a container.
     * @param IContainer container 
     * @param onlyVideo - whether IndexEntries of video stream only should be used
     * @return
     * @throws Exception 
     */
    private static TreeMap<Long, Long> getTreeMapOfIndexEntries(IContainer container, boolean onlyVideo ) throws Exception {
	    
	    // query how many streams the call to open found
	    int numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new Exception( "No streams found in container." );
	    
	    LOG.debug( "numStreams: " + numStreams );
	    
	    if( onlyVideo ) numStreams = 1;
	    
	    // all_indices will contain all byte positions of index entries of all streams in sequential order
	    TreeMap<Long, Long> all_indices = new TreeMap<Long, Long>();
	    
	    
	    // get lists of index entries of the streams
	    List<IIndexEntry> index_list[] = new List[numStreams];
	    boolean[] get_keyframes = new boolean[numStreams];
	    int list_done = 0;
	    
	    int i = 0;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        
	        LOG.debug( "stream " + j + ": " + coder.getCodecType().name() );
	        if( onlyVideo && coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list[i] = stream.getIndexEntries();
	        if( index_list[i].size() == 0 ) list_done++;
	        
	        LOG.debug( "index-list-size = " + index_list[i].size() );
	        
	        if(  LOG.isDebugEnabled() ) {
		        String output = "Index List " + i + ": Size " + index_list[i].size() + " : ";
		        String list = ""; 
		        for( int k = 0; k < index_list[i].size(); k++ )
		        {
		            //output += index_list[i].get(k).getPosition() + ", ";
		            list += index_list[i].get(k).getTimeStamp() + ":" + index_list[i].get(k).getPosition() + "\n";
		        }
		        /*
		        File f = File.createTempFile("indices_readable_", ".tmp");
		        try {
                    FileOutputStream fos = new FileOutputStream( f );
                    fos.write( list.getBytes() );
                    fos.flush();
                    fos.close();
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                */
		        
		        LOG.debug( list );
		        
	        }
	        
	        // if the stream is a video stream get_keyframes flag is set
	        get_keyframes[i] = coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO;
	        LOG.debug( "get_keyframes : " + get_keyframes[i] );
	        i++;
	        
	        // video stream was found. if "onlyVideo" then don't look at any other streams
	        if( onlyVideo) break;
	    }
	    
	    int counter[] = new int[numStreams];
	    for( int j = 0; j < numStreams; j++ ) counter[j] = 0;
	    
	    // iterate over all streams and put the smallest (key-)index-entry of the current index-entries of the streams
	    // into all_indices-list. stops when all list have been parsed.
	    while( list_done < numStreams ) {
	        long frame_no = -1;
	        long min_pos = -1;
	        int list_nr = -1;
	        for( int j = 0; j < numStreams; j++ ) {
	            if( index_list[j].size() > counter[j] ) {
	                
	                // if get_keyframes is true for this stream move up to next keyframe
	                // but at the beginning put the smallest index entry at first position (see first boolean statement)
	                while( counter[j] != 0 &&  
	                        get_keyframes[j] && 
	                        counter[j] < index_list[j].size() && 
	                        !index_list[j].get(counter[j]).isKeyFrame() ) counter[j]++;
	                if( counter[j] >= index_list[j].size() ) {
	                    list_done++;
	                    continue;
	                }
	                                                                                        
	                // get index with smallest current_index-position 
	                IIndexEntry current_index = index_list[j].get(counter[j]);
	                if( min_pos == -1 || current_index.getPosition() < min_pos )
	                {
	                    min_pos = current_index.getPosition();
	                    frame_no = current_index.getTimeStamp();
	                    list_nr = j;
	                }
	            }
	        }
	        
	        if( list_nr == -1 ) break;
            counter[list_nr]++;
	        if( index_list[list_nr].size() <= counter[list_nr] ) list_done++;
	        
	        all_indices.put( min_pos, frame_no );
	    }
	    // end-of-file is also treated a index entry
	    // frame_no = -1, because it does not matter ;)
	    all_indices.put( container.getFileSize(), -1L );
	    LOG.debug( "all_indices.size = " + all_indices.size() );
        return all_indices;
    }
    
    
    
    static public String cacheIndexEntries( IContainer container ) throws Exception {
        
        //List<Long> indices = getIndexEntries( container, true );
        TreeMap<Long, Long> indices = getTreeMapOfIndexEntries( container, true );
        if( LOG.isDebugEnabled() ) {
	        String output = "All Indices: Size " + indices.size() + " sorted:";
	        Iterator<Entry<Long,Long>> it = indices.entrySet().iterator();
	        while( it.hasNext() ) 
	            output += it.next() + "\n";
	        LOG.debug( output );
        }
        
        // serialize indices-list into file
        File index_file = File.createTempFile("indices_", ".tmp" );
        
        FileOutputStream fos = new FileOutputStream( index_file );
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject( indices );
        
        oos.flush();
        oos.close();
        
        fos.flush();
        fos.close();
        
        return index_file.toString();
    }
}
