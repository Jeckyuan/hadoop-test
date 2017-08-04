/**
 * @author Jeck
 *
 * Create on 2017年8月2日 下午2:33:58
 */
package com.common.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class HDFSFileUtil {
    private static Logger log = Logger.getLogger(HDFSFileUtil.class);

    private static Configuration conf = null;
    private static FileSystem fs = null;

    public static void main(String[] args) throws Exception {
	String localSrc = args[0];
	String dst = args[1];
	conf = new Configuration();
	fs = FileSystem.get(URI.create(dst), conf);

	OutputStream out = fs.create(new Path(dst), new Progressable() {
	    @Override
	    public void progress() {
		System.out.print(".");
	    }
	});

	BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(out));


	//	IOUtils.copyBytes(in, out, 4096, true);
	System.out.print("start to write...");
	for(int i=0; i<100; i++){
	    bw.write(localSrc);
	}
	System.out.print("write completed.");
	bw.close();
    }

    public static ArrayList<String> fileRead(String filePath) {
	int lineCount = 0;
	ArrayList<String> keyArray = new ArrayList<>();
	try (InputStream in = fs.open(new Path(filePath))) {
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String tmpLine = null;
	    while ((tmpLine = br.readLine()) != null) {
		String[] lineSplit = tmpLine.trim().split("\t");
		if (lineSplit.length == 2) {
		    keyArray.add(lineSplit[0]);
		    lineCount++;
		}
	    }
	    System.out.println(filePath + ": " + lineCount);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return keyArray;
    }

    /**
     * 输出指定IMEI前缀对应的所有IMEI到指定的路径
     *
     * @param imeiPrefixArray
     * @param filePath
     */
    public static void writeAllImei(ArrayList<String> imeiPrefixArray, String filePath) {
	// Long st = 0L;
	Long ed = 1000000L;

	try (OutputStream out = fs.create(new Path(filePath))) {
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

	    for (String pre : imeiPrefixArray) {
		if (pre.trim().length() == 6) {
		    for (Long ind = 0L; ind < ed; ind++) {
			bw.write(pre + String.format("%1$04d", ind));
			bw.newLine();
		    }
		} else {
		    log.error("该IMEI前缀格式不正确：" + pre);
		}
	    }
	} catch (Exception e) {
	    log.error(e.getMessage());
	    e.printStackTrace();
	}

    }

}
