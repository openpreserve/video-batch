package at.ac.ait.dme.video_batch.xuggle;

import at.ac.ait.dme.video_batch.AVMediaFramework;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IIndexEntry;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.io.URLProtocolManager;
import java.math.BigInteger;
import java.net.URI;
import java.util.List;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Comprises some utility methods for pre-processing AV files using Xuggler.
 *
 * @author Matthias Rella, DME-AIT
 */
public class XugglerMediaFramework implements AVMediaFramework {

    private static final Log LOG = LogFactory.getLog(XugglerMediaFramework.class);
    private static URLProtocolManager mgr = URLProtocolManager.getManager();

    private IContainer container;
    private int numStreams;

    public void init(URI file) throws RuntimeException {

        LOG.debug( "Input file: " + file );

        if( file.toString().startsWith("hdfs"))
            mgr.registerFactory("hdfs", new HDFSProtocolHandlerFactory());

        container = IContainer.make();
        
        int r = container.open(file.toString(), IContainer.Type.READ, null);

        if( r < 0)
        {
          IError error = IError.make( r );
	      throw new IllegalArgumentException("could not open container from " + file + ", error = " + error.getDescription() );
        }

	    numStreams = container.getNumStreams();
	    
	    if( numStreams <= 0 )
	        throw new IllegalArgumentException( "No streams found in container." );
    }

    public TreeMap<Long, Long> getIndices( boolean videoOnly ) {

        int numStreams = this.numStreams;
	    if( videoOnly ) numStreams = 1;
	    
	    // all_indices will contain all byte positions of index entries of stream(s) in sequential order
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
	        if( videoOnly && coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list[i] = stream.getIndexEntries();
	        if( index_list[i].isEmpty() ) list_done++;
	        
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
	        if( videoOnly) break;
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
	    // end-of-file is also treated as index entry
	    // frame_no = -1, because it does not matter 

	    all_indices.put( container.getFileSize(), -1L );
	    LOG.debug( "all_indices.size = " + all_indices.size() );
        return all_indices;
    }

    public long getAvgGOPSize() {
	    List<IIndexEntry> index_list = null;
	    
        IStream videostream = null;
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list = stream.getIndexEntries();
            videostream = stream;
	    }
	    
        // TODO: generalize MediaFramework interface to deal with streamed video types
        // in this case here getFileSize only works for non-streamed containers which support seek()
	    if( index_list == null || index_list.size() == 0)
            return container.getFileSize();
	    
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

    public long getAvgGOPLength(){
	    
	    List<IIndexEntry> index_list = null;

        IStream videostream = null;
	    
	    for( int j = 0; j < container.getNumStreams(); j++ ) {
	        IStream stream = container.getStream(j);
	        IStreamCoder coder = stream.getStreamCoder();
	        if( coder.getCodecType() != ICodec.Type.CODEC_TYPE_VIDEO)
	            continue;
	        index_list = stream.getIndexEntries();
            videostream = stream;
	    }
	    
        // TODO: generalize MediaFramework interface to deal with audio too (where not videostream is contained)
	    if( index_list == null || index_list.isEmpty() )
            return videostream.getNumFrames();
	    
	    int k = 0;
	    for( int i = 0; i < index_list.size(); i++ )
	       if ( index_list.get(i).isKeyFrame() ) k++; 
	    
        return index_list.size() / k;
    }

    public String getCodecPackageName() {
        return "at.ac.ait.dme.video_batch.xuggle.XugglerCodec";
    }
}
