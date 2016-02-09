package at.ac.ait.dme.video_batch.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RGBTable {

	private Configuration config = null;
	private static RGBTable rgbTable = null;
	private String name = "rgbTable";
	private String family = "family1";
	private HTable table = null;
	long counter = 0L;


	private static final Log LOG = LogFactory.getLog(RGBTable.class);

	private RGBTable() {
		init();
	}

	private void init() {
		try {
			config = HBaseConfiguration.create();
						
		    /*
			Iterator<Entry<String,String>> it = config.iterator();
			LOG.info("Here is the HBaseConfiguration:");
			while (it.hasNext()) {
				Entry<String,String> e = it.next();
				LOG.info(e.getKey()+"="+e.getValue());
			}
			*/
			
			LOG.info("About to create HBaseAdmin. Zookeeper Quorum in Conf is: "+config.get("hbase.zookeeper.quorum"));
			HBaseAdmin hAdmin = new HBaseAdmin(config);

			if (!hAdmin.tableExists(this.name.getBytes())) {
				HTableDescriptor tdesc = new HTableDescriptor(
						Bytes.toBytes(this.name));
				HColumnDescriptor cdesc_family1 = new HColumnDescriptor(
						Bytes.toBytes(this.family));
				//HColumnDescriptor cdesc_family2 = new HColumnDescriptor(
				//		Bytes.toBytes(this.family2));
				tdesc.addFamily(cdesc_family1);
				//tdesc.addFamily(cdesc_family2);
				hAdmin.createTable(tdesc);
			}
			table = new HTable(config, this.name);
			hAdmin.close();
		} catch (IOException e) {
			LOG.warn("Unable to create HBase RGBTable: ", e);
		}
	}

	static public RGBTable getInstance() {
		if (rgbTable == null) {
			rgbTable = new RGBTable();
		}
		return rgbTable;
	}

	public String getName() {
		return name;
	}
	
	public String getFamily() {
		return family;
	}
	
	public void putRGB(String jobId, int red, int green, int blue, int alpha) {
		String[] colors = {"red", "green", "blue", "alpha"};
		int[] values = {red, green, blue, alpha};
		String key = jobId+"_"+(counter++);
		int byteRepresentation = getByteRepresentation(red, green, blue, alpha);
		Put put = new Put(Bytes.toBytes(key)); //now row key specified		
		for(int i = 0; i<colors.length;i++) {
			put.add(Bytes.toBytes(this.family), Bytes.toBytes("col_"+colors[i]), Bytes.toBytes(values[i]));
		}
		put.add(Bytes.toBytes(this.family), Bytes.toBytes("jobid"), Bytes.toBytes(jobId));
		int allValues = getByteRepresentation(red, green, blue, alpha);
		put.add(Bytes.toBytes(this.family), Bytes.toBytes("allvals"), Bytes.toBytes(allValues));
		try {
			table.put(put);
		} catch (IOException e) {
			LOG.warn("error wrting rgb to table: ", e);
		}
	}
	
	private int getByteRepresentation(int red, int green, int blue, int alpha) {
		return red<<24 | green<<16 | blue<<8 | alpha;
	}
	
	private boolean isRowExisting(String key) throws IOException {
		Get g = new Get(Bytes.toBytes(key));
		Result r = table.get(g);
		return !r.isEmpty();
	}

}


//int color = image.getRGB(image.getHeight()/2, image.getWidth()/2);
//color == 0xAARRGGBB
//int r = (argb)&0xFF;         1010 1101 1110 0111 1010 1101 1110 0111
//int g = (argb>>8)&0xFF;      1111 1111 1010 1101 1110 0111 1010 1101 
//int b = (argb>>16)&0xFF;     1111 1111 1111 1111 1010 1101 1110 0111 
//int a = (argb>>24)&0xFF;     1111 1111 1111 1111 1111 1111 1010 1101 
//          
//System.out.println(" color: "+ Integer.toBinaryString(color));
//System.out.println("  c>>8: "+ Integer.toBinaryString(color>>8));
//System.out.println(" c>>16: "+ Integer.toBinaryString(color>>16));
//System.out.println(" c>>24: "+ Integer.toBinaryString(color>>24));
//System.out.println("RGBA: "+((color)&0xFF)+" "+((color>>8)&0xFF)+" "+((color>>16)&0xFF)+" "+((color>>24)&0xFF));
//System.out.println("RGBA: "+ccolor.getBlue()+" "+ccolor.getGreen()+" "+ ccolor.getRed()+" "+ccolor.getAlpha());            
