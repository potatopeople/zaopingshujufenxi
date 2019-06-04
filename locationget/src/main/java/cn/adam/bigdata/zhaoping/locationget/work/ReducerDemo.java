package cn.adam.bigdata.zhaoping.locationget.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.locationget.entity.Location;
import cn.adam.bigdata.zhaoping.util.Utils;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ReducerDemo extends DefaultReducer<Text, JobWritable, Text, NullWritable> {
	public static final String LOCATIONFILEPATH = "adam_locationfilepath";
	private Map<String, Location> locationMap = new HashMap<>();

	@Override
	protected void reduce(Text key, Iterable<JobWritable> values,
						  Context context)
			throws IOException, InterruptedException {

		Location location = locationMap.get(key.toString());

		for (JobWritable j : values) {
        	if (location != null) {
				j.setCompany_location_province(location.getProvince());
				j.setCompany_location_city(location.getCity());
				j.setCompany_location_district(location.getDistrict());
				j.setCompany_location_longitude(location.getLongitude().toString());
				j.setCompany_location_latitude(location.getLatitude().toString());
			}
            Utils.emptyFieldToNull(j);
			context.write(new Text(j.toString()), NullWritable.get());
		}

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		getLocation(context);
	}

	public void getLocation(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();
		FileSystem fileSystem = FileSystem.get(configuration);
		InputStream open = fileSystem.open(new Path(configuration.get(LOCATIONFILEPATH)));
		try (Scanner sc = new Scanner(open)){
			while (sc.hasNextLine()){
				String s = sc.nextLine();
				Location location = getLocation(s);
				locationMap.put(location.getLocation(), location);
			}
		}catch (Exception e){
			throw e;
		}
	}

	public static Location getLocation(String s) throws IOException {
		Location location = new Location();
		s = s.replaceAll("\\[]", "");
		String[] ss = s.split("\t");

		if (ss[0].equals(""))ss[0] = ss[5];
		location.setProvince(ss[0]);
		if (ss[1].equals(""))ss[1] = null;
		location.setCity(ss[1]);
		if (ss[2].equals(""))ss[2] = null;
		location.setDistrict(ss[2]);
		location.setLongitude(Double.parseDouble(ss[3]));
		location.setLatitude(Double.parseDouble(ss[4]));
		location.setLocation(ss[5]);
		return location;
	}
}
