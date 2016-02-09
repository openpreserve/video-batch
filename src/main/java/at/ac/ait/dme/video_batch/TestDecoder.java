package at.ac.ait.dme.video_batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.xuggle.xuggler.Global;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IError;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.ICodec.Type;

public class TestDecoder {

	private InputStream videoStream = null;
	private String filename = null;
	private IContainer container;
	
	private IStream[] streams = null;
	private IStreamCoder[] coders = null;
	private Type[] streamTypes = null;


	public TestDecoder(String video) {
		this.filename = video;
		File videoFile = new File(video);
		try {
			videoStream = new FileInputStream(videoFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init() {
		this.container = IContainer.make();
		//container.open(videoStream, null);
		container.open(filename, IContainer.Type.READ, null);

		int r = this.container.open(videoStream, null);

		System.out.println("container.open = " + r);

		if (r < 0) {
			IError error = IError.make(r);
			System.out.println("error: " + error.getDescription());
			try {
				throw new IOException("Could not create Container from given InputStream");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		int numStreams = this.container.getNumStreams();
		this.streams = new IStream[numStreams];
		this.coders = new IStreamCoder[numStreams];
		this.streamTypes = new Type[numStreams];
		System.out.println("Detected streams n=" + numStreams);

		for (int s = 0; s < this.streams.length; s++) {
			this.streams[s] = this.container.getStream(s);
			printStreamInfo(streams[s]);
			this.coders[s] = this.streams[s] != null ? this.streams[s]
					.getStreamCoder() : null;
			this.streamTypes[s] = this.coders[s] != null ? this.coders[s]
					.getCodecType() : null;
		}
		// testDecoding();
	}

	private void printStreamInfo(IStream stream) {
		IStreamCoder coder = stream.getStreamCoder();
		IContainer container = stream.getContainer();
		String info = "";

		info += (String.format("type: %s; ", coder.getCodecType()));
		info += (String.format("codec: %s; ", coder.getCodecID()));
		info += String.format(
				"duration: %s; ",
				stream.getDuration() == Global.NO_PTS ? "unknown" : ""
						+ stream.getDuration());
		info += String.format(
				"start time: %s; ",
				container.getStartTime() == Global.NO_PTS ? "unknown" : ""
						+ stream.getStartTime());
		info += String.format("language: %s; ",
				stream.getLanguage() == null ? "unknown" : stream.getLanguage());
		info += String.format("timebase: %d/%d; ", stream.getTimeBase()
				.getNumerator(), stream.getTimeBase().getDenominator());
		info += String.format("coder tb: %d/%d; ", coder.getTimeBase()
				.getNumerator(), coder.getTimeBase().getDenominator());

		if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_AUDIO) {
			info += String.format("sample rate: %d; ", coder.getSampleRate());
			info += String.format("channels: %d; ", coder.getChannels());
			info += String.format("format: %s", coder.getSampleFormat());
		} else if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
			info += String.format("width: %d; ", coder.getWidth());
			info += String.format("height: %d; ", coder.getHeight());
			info += String.format("format: %s; ", coder.getPixelType());
			info += String.format("frame-rate: %5.2f; ", coder.getFrameRate()
					.getDouble());
		}
		System.out.println(info);
	}

}
