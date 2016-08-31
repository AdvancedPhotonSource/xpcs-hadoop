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

package gov.anl.aps.xpcs.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.mapred.job.FrameSumFileFilter;
import gov.anl.aps.xpcs.mapred.job.SumsFileFilter;

public class ResultCollector implements Runnable {

	private static final Logger logger = LoggerFactory
			.getLogger(ResultCollector.class.getName());

	private static final String NEWLINE = "\n";
	private static final String SEP = ",";

	private XPCSConfig config = null;

	private QMaps qmaps = null;

	private int staticPartitions = 0;

	private short[][] staticMappings = null;

	private double[][] partitionSums;

	private int frameWindow;

	private int frameCount;

	private int[] staticPartitionsCount;

	private boolean isKinetics;

	private int kineticSlices;

	public ResultCollector(XPCSConfig config) {
		this.config = config;
		qmaps = this.config.getMaps();
		this.staticPartitions = this.qmaps.getTotalStaticPartitions();
		this.staticPartitionsCount = this.qmaps.getStaticCounts();
		this.staticMappings = this.qmaps.getStaticMapping();
		this.frameWindow = this.config.getStaticWindow();
		this.isKinetics = this.config.getIsKinetics();
		this.kineticSlices = this.config.getLastFrame()
				- this.config.getFirstFrame() + 1;
		this.frameCount = this.config.getFramecount();
		if (this.isKinetics) {
			this.frameCount *= this.kineticSlices;
		}
	}

	@Override
	public void run() {
		logger.info("Running result collector thread");

		if (this.qmaps == null)
			return;

		String hdfOutputDir = this.config.getOutputDir() + "/g2";
		boolean check = checkOrCreateDir(this.config.getOutputDir());
		if (!check) {
			logger.error("Failed to generate output form HDFS. \n The path to "
					+ " the output directory is either not valid \n Or this program "
					+ "doesn't have premission to create one");
			return;
		}

		int windowCount = 0;
		if (frameWindow > 0 && frameWindow != frameCount) {

			windowCount = (int) Math.floor((float) this.frameCount
					/ (float) this.frameWindow);

		}

		// Window goes from 1 to N, the +1 is to handle that
		// static partitions are already have +1
		partitionSums = new double[staticPartitions][windowCount + 1];

		try {
			readFrameInfo(hdfOutputDir);
		} catch (IOException e) {
			logger.error("Failed to read frame-info from the finished job"
					+ e.getMessage());
			if (logger.isDebugEnabled()) {
				e.printStackTrace();
			}
		}

		try {
			readPixelSums(hdfOutputDir);
		} catch (IOException e) {
			logger.error("Failed to read pixel-sums from the finished job"
					+ e.getMessage());
			if (logger.isDebugEnabled()) {
				e.printStackTrace();
			}
		}

		try {
			computePartitionMean(partitionSums, windowCount);
		} catch (IOException e) {
			logger.error("Failed to compute partitions sums using the "
					+ "pixels-sum from the finished job" + e.getMessage());
			if (logger.isDebugEnabled()) {
				e.printStackTrace();
			}
		}
	}

	private boolean checkOrCreateDir(String output) {

		File f = new File(output);

		if (f.exists() && f.isDirectory()) {
			return true;
		}

		if (f.exists() && !f.isDirectory()) {
			return false;
		}

		if (!f.exists() && f.mkdirs())
			return true;

		return false;
	}

	private void readFrameInfo(String hdfsOutput) throws IOException {
		FileSystem hdfs = FileSystem.get(this.config);
		FileStatus frameSums[] = hdfs.listStatus(new Path(hdfsOutput),
				new FrameSumFileFilter());

		StringBuilder outBuilder = new StringBuilder(this.frameCount * 40);
		// Local file where sums/time-stamps will be stored
		String outFile = this.config.getOutputDir() + "/" + "frameData";
		logger.info("Frame data output dir: " + outFile);

		BufferedReader buffin = null;
		for (FileStatus src : frameSums) {
			FSDataInputStream fIn = hdfs.open(src.getPath());
			buffin = new BufferedReader(new InputStreamReader(fIn));
			String line = null;
			while ((line = buffin.readLine()) != null) {
				String[] split = line.split(",");
				
				if (split.length != 4)
					continue;

				int frame = Integer.parseInt(split[0].trim());
				double sum = normalizeSums(Double.parseDouble(split[1].trim()));
				double frameClock = Double.parseDouble(split[2].trim());
				double frameTick = Double.parseDouble(split[3].trim());
				append(outBuilder, frame, sum, frameClock, frameTick);
			}
		}
		flush(outFile, outBuilder);
	}

	private void readPixelSums(String hdfOutputDir) throws IOException {
		FileSystem hdfs = FileSystem.get(this.config);
		FileStatus pixelSums[] = hdfs.listStatus(new Path(hdfOutputDir),
				new SumsFileFilter());
		StringBuilder outBuilder = new StringBuilder(this.staticPartitions * 40);
		String outFile = this.config.getOutputDir() + "/" + "pixelSums";

		BufferedReader buffin = null;
		for (FileStatus src : pixelSums) {
			FSDataInputStream fIn = hdfs.open(src.getPath());
			buffin = new BufferedReader(new InputStreamReader(fIn));
			String line = null;
			while ((line = buffin.readLine()) != null) {
				String[] split = line.split(",");
				if (split.length != 4)
					continue;
				int x = Integer.parseInt(split[0].trim());
				int y = Integer.parseInt(split[1].trim());
				int window = Integer.parseInt(split[2].trim());
				double sum = Double.parseDouble(split[3].trim());
				int staticPartID = staticMappings[x][y];
				if (window > partitionSums[staticPartID].length)
					continue;
				partitionSums[staticPartID][window] += sum;

				// pixel sum over all frames
				if (window == 0) {
					append(outBuilder, x, y, sum
							/ (double) this.frameCount);
				}
			}
		}

		flush(outFile, outBuilder);
	}

	private void computePartitionMean(double[][] sums, int windowCount)
			throws IOException {
		StringBuilder outBuilder = new StringBuilder(this.staticPartitions * 40);
		String outFile = this.config.getOutputDir() + "/" + "partition-mean";

		double partitionNorm = PartitionMeanNormFactor
				.computeNormFactor(config);

		if (Double.isNaN(partitionNorm))
			partitionNorm = 1.0;

		// TODO: Log error or warn
		if (sums.length < 0)
			return;
		if (sums[0].length < windowCount + 1)
			return;

		// Write out partition means for intermediate frame windows.
		for (int i = 1; i <= windowCount; i++) {
			for (int j = 1; j < this.staticPartitions; j++) {
				double mean = (double) sums[j][i]
						/ (double) (staticPartitionsCount[j] * (this.frameWindow));
				append(outBuilder, j, ((i - 1) * this.frameWindow),
						this.frameWindow - 1, mean / partitionNorm);
			}
		}

		// Write out partition means for all frames
		for (int j = 1; j < staticPartitions; j++) {
			double mean = (double) sums[j][0]
					/ (double) (staticPartitionsCount[j] * (this.frameCount));
			append(outBuilder, j, 0, this.frameCount - 1, mean / partitionNorm);
		}

		flush(outFile, outBuilder);
	}

	private void append(StringBuilder outBuilder, int partition, int start,
			int end, double mean) {
		outBuilder.append(partition);
		outBuilder.append(SEP);
		outBuilder.append(start);
		outBuilder.append(SEP);
		outBuilder.append(start + end);
		outBuilder.append(SEP);
		outBuilder.append(mean);
		outBuilder.append(NEWLINE);
	}

	private void append(StringBuilder outBuilder, int frame, double sum,
			double clock, double time) {
		outBuilder.append(frame);
		outBuilder.append(SEP);
		outBuilder.append(sum);
		outBuilder.append(SEP);
		outBuilder.append(clock);
		outBuilder.append(SEP);
		outBuilder.append(time);
		outBuilder.append(NEWLINE);
	}

	private void append(StringBuilder outBuilder, int x, int y, double sum) {
		outBuilder.append(x);
		outBuilder.append(SEP);
		outBuilder.append(y);
		outBuilder.append(SEP);
		outBuilder.append(sum);
		outBuilder.append(NEWLINE);
	}

	private void flush(String outFile, StringBuilder outputBuilder)
			throws IOException {
		Writer outputWriter = new OutputStreamWriter(new BufferedOutputStream(
				new FileOutputStream(outFile), 16 * 1024));
		outputWriter.write(outputBuilder.toString());
		outputWriter.flush();
		outputWriter.close();
	}

	private double normalizeSums(double sum) {
		if (this.config == null)
			return sum;

		return (double) sum / this.config.getDetEfficiency()
				/ this.config.getDetAdhupPhot() / this.config.getDetPreset();
	}
}
