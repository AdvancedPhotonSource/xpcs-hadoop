/*
* Copyright (c) 2016, UChicago Argonne, LLC. All rights reserved.
*
* Copyright 2016. UChicago Argonne, LLC. This software was produced 
* under U.S. Government contract DE-AC02-06CH11357 for Argonne National 
* Laboratory (ANL), which is operated by UChicago Argonne, LLC for the 
* U.S. Department of Energy. The U.S. Government has rights to use, 
* reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR 
* UChicago Argonne, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR 
* ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is 
* modified to produce derivative works, such modified software should 
* be clearly marked, so as not to confuse it with the version available 
* from ANL.

* Additionally, redistribution and use in source and binary forms, with 
* or without modification, are permitted provided that the following 
* conditions are met:
*
*   * Redistributions of source code must retain the above copyright 
*     notice, this list of conditions and the following disclaimer. 
*
*   * Redistributions in binary form must reproduce the above copyright 
*     notice, this list of conditions and the following disclaimer in 
*     the documentation and/or other materials provided with the 
*     distribution. 
*
*   * Neither the name of UChicago Argonne, LLC, Argonne National 
*      Laboratory, ANL, the U.S. Government, nor the names of its 
*      contributors may be used to endorse or promote products derived 
*      from this software without specific prior written permission. 

* THIS SOFTWARE IS PROVIDED BY UChicago Argonne, LLC AND CONTRIBUTORS 
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
* FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL UChicago 
* Argonne, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
* BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
* CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
* LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
* ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
* POSSIBILITY OF SUCH DAMAGE.
*/

package gov.anl.aps.xpcs.main;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.config.XPCSHDFConfig;

import gov.anl.aps.xpcs.mapred.IMMRecordReader;
import gov.anl.aps.xpcs.mapred.job.G2JobConf;
import gov.anl.aps.xpcs.mapred.job.NormJobConf;
import gov.anl.aps.xpcs.mapred.job.TwoTimesJobConf;
import gov.anl.aps.xpcs.mapred.filter.FilterMain;

import gov.anl.aps.xpcs.util.DarkImage;
import gov.anl.aps.xpcs.util.FlatField;
import gov.anl.aps.xpcs.util.QMaps;
import gov.anl.aps.xpcs.util.FrameSum;
import gov.anl.aps.xpcs.util.PixelSum;
import gov.anl.aps.xpcs.util.SmoothedSG;
import gov.anl.aps.xpcs.util.ResultCollector;
import gov.anl.aps.xpcs.util.HDF5ResultHelper;
import gov.anl.aps.xpcs.mapred.io.Frame;

import gov.anl.aps.xpcs.mapred.io.FrameValue;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugJob extends Configured implements Tool {

	/**
	 * Slf4j logger
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(DebugJob.class.getName());

	/**
	 * Main application configuration object
	 */
	private XPCSConfig configuration = null;

	private IMMRecordReader reader = null;
	/**
	 * Debugging use.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		DebugJob app = new DebugJob();
		int res = ToolRunner.run(app, args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		configuration = new XPCSConfig(getConf());

		//configuration.debug();
		XPCSHDFConfig hdf5Config = new XPCSHDFConfig(configuration);
		System.out.println(configuration.getAnalysisType());

		// Path inputPath = new Path(this.configuration.getOutputDir() + "/g2");
		// FileSystem hdfs = FileSystem.get(this.configuration);
		// FileStatus[] fls = hdfs.listStatus(inputPath);

		// HDF5ResultHelper.writeTwoTime(configuration);


		// int frameCount = configuration.getFramecount();
		// byte[] buffer = new byte[8 + (4 * frameCount * frameCount)];
		
		// long bytesRead = 0;

		// for (FileStatus fl : fls) {
		// 	if (fl.getPath().getName().contains("part-"))
		// 	{
		// 		BufferedInputStream buffin = new BufferedInputStream(hdfs.open(fl.getPath()));
		// 		bytesRead = buffin.read(buffer, 0, buffer.length);
		// 		System.out.println("Read " + bytesRead + " for " + fl.getPath());
		// 		ByteBuffer data = ByteBuffer.wrap(buffer, 0, (int)bytesRead);
		// 		int q = (int) data.getDouble();
		// 	}
		// }

		// String output = configuration.getOutputDir();
		// HDF5ResultHelper.writeTwoTime(configuration, output);

		QMaps map = configuration.getMaps();
		map.writeLocal("dqmap", "sqmap");
		map.writeLocalASCII("qmap.txt");

		if (true) return 0;
		// if (map == null) {
		// 	logger.error("Failed to read partition data from the configuraiton");
		// 	return -1;
		// }

		// short [][] dmap = map.getDynamicMapping();

		// int bin = 1;

		// ArrayList<Integer> pixelsInBin = new ArrayList<Integer>();
		
		// int width = configuration.getFrameWidth();
		// int height = configuration.getFrameHeight();

		// System.out.println(width + " , " + height);

		// int cnt = 0;
		// for (int i = 0; i < height; i++) {
		// 	for (int j = 0; j < width; j++) {
		// 		if (dmap[j][i] == bin) {
		// 			pixelsInBin.add(i * width + j);
		// 			cnt++;
		// 		}
		// 	}
		// }

		// System.out.println("Found " + cnt + " pixels in bin # " + bin);

		// SmoothedSG sg = configuration.getSmoothedSG();
		// double data[] = sg.getSmoothedSG();
		// double sum = 0.0;

		// cnt = 0;
		// for (int i : pixelsInBin) {
		// 	sum += data[i];
		// 	cnt++;
		// }

		// System.out.println("sum = " + sum);

		// FrameSum fsum = configuration.getFrameSum();

		// double newsum[] = fsum.getFrameSum();

		// // for (int i = 0 ; i < newsum.length; i++)
		// // 	System.out.println(newsum[i]);

		// if (true) return 0;

		// // SmoothedSG pSum = new SmoothedSG(configuration); //configuration.getSmoothedSG();
		// // data = pSum.getSmoothedSG();
		// // for (int i = 7852 ; i < 7859; i++)
		// // 	System.out.println(data[i]);


		// // if (true) return 0;
		// // for (int i = 0 ; i < (1024*1024); i++) {
		// // 	if ( (i % 10) == 0) System.out.println();
		// // 	System.out.print(i + ":" + data[i] + " ,");
		// // }

		

		// // Un-comment following to debug the qmap generation
		// // TODO: Remove hard-coded names for the map dire
		// // map.writeHDFS("dqmap", "sqmap", configuration);

		DarkImage darkImage = null;
		if (!this.configuration.getIsSparse()) {
			darkImage = DarkImage.initFromIMMFile(configuration);

			darkImage.writeToLocal("darkAverages", "darkStds");
		}

		
		if (this.configuration.getIsFlatFieldEnabled()) {
			FlatField f = configuration.getFlatField();
			f.writeToLocal("flatfield");
		}

		Path input = new Path(configuration.getInputFilePath());
		FileSystem fsys = input.getFileSystem(configuration);
		FileStatus stat = fsys.getFileStatus(input);

		logger.info("File input path " + input.toString() + " size (in bytes) = " + stat.getLen());

		JobConf imposter = new JobConf(configuration);
		
		FileSplit fs = new FileSplit(input, 0, stat.getLen(), imposter);
		reader = new IMMRecordReader(fs, imposter, Reporter.NULL);			
		// FilterMain filters = new FilterMain(this.configuration);
	
		for (int k = 0 ; k < 2; k++)  
		{
			Frame frame = reader.getFrame();
			float f[] = frame.getPixels();
			
			// for (int i = 0  ; i < f.length; i++)
			// 	System.out.println(f[i]);

			System.out.println(frame.getFrameSum());
			reader.nextFrame();
		}

		return 0;
	}	
}
