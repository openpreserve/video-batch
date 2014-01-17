package at.ac.ait.dme.video_batch.hadoop20workaround;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Workaround for Hadoop 0.20. 
 * This class exists in Hadoop 0.21
 */
public interface SplittableCompressionCodec extends CompressionCodec {

  /**
   * During decompression, data can be read off from the decompressor in two
   * modes, namely continuous and blocked.  Few codecs (e.g. BZip2) are capable
   * of compressing data in blocks and then decompressing the blocks.  In
   * Blocked reading mode codecs inform 'end of block' events to its caller.
   * While in continuous mode, the caller of codecs is unaware about the blocks
   * and uncompressed data is spilled out like a continuous stream.
   */
  public enum READ_MODE {CONTINUOUS, BYBLOCK};

  /**
   * Create a stream as dictated by the readMode.  This method is used when
   * the codecs wants the ability to work with the underlying stream positions.
   *
   * @param seekableIn  The seekable input stream (seeks in compressed data)
   * @param start The start offset into the compressed stream. May be changed
   *              by the underlying codec.
   * @param end The end offset into the compressed stream. May be changed by
   *            the underlying codec.
   * @param readMode Controls whether stream position is reported continuously
   *                 from the compressed stream only only at block boundaries.
   * @return  a stream to read uncompressed bytes from
   */
  SplitCompressionInputStream createInputStream(InputStream seekableIn,
      Decompressor decompressor, long start, long end, READ_MODE readMode)
      throws IOException;

}
