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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import gov.anl.aps.xpcs.config.XPCSConfig;
import gov.anl.aps.xpcs.config.XPCSHDFConfig;
import gov.anl.aps.xpcs.mapred.job.G2JobConf;
import gov.anl.aps.xpcs.mapred.job.NormJobConf;
import gov.anl.aps.xpcs.mapred.job.TwoTimesJobConf;
import gov.anl.aps.xpcs.util.DarkImage;
import gov.anl.aps.xpcs.util.FlatField;
import gov.anl.aps.xpcs.util.QMaps;
import gov.anl.aps.xpcs.util.FrameSum;
import gov.anl.aps.xpcs.util.PixelSum;
import gov.anl.aps.xpcs.util.SmoothedSG;
import gov.anl.aps.xpcs.util.ResultCollector;
import gov.anl.aps.xpcs.util.HDF5ResultHelper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application extends Configured implements Tool {

	/**
	 * Slf4j logger
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(Application.class.getName());

	/**
	 * Main application configuration object
	 */
	private XPCSConfig configuration = null;

	private boolean stopRequested = false;

	private boolean running = false;

	/**
	 * Debugging use.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		Application app = new Application();		
		int res = ToolRunner.run(app, args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		running = true;

		// Handle Ctl+C events
		TheShutdownHook hook = new TheShutdownHook(this);
		Thread t = new Thread(hook);
		Runtime.getRuntime().addShutdownHook(t);

		configuration = new XPCSConfig(getConf());

		// TODO XPCSHDFConfig is a lonely object, fix it.
		XPCSHDFConfig hdf5Config = new XPCSHDFConfig(configuration);

		QMaps map = configuration.getMaps();
		if (map == null) {
			logger.error("Failed to read partition data from the configuraiton");
			return -1;
		}

		// Un-comment following to debug the qmap generation
		map.writeLocalASCII("qmap.txt");

		// TODO: Remove hard-coded names for the map dire
		map.writeHDFS(
			configuration.getOutputDir() + "/dqmap", 
			configuration.getOutputDir() + "/sqmap", 
			configuration);

		if (!this.configuration.getIsSparse()) {
			DarkImage darkImage = DarkImage.initFromIMMFile(configuration);
			darkImage.writeToHDFS(
				configuration.getOutputDir() + "/darkAverages", 
				configuration.getOutputDir() + "/darkStds",
				configuration);
		}
	
		if (this.configuration.getIsFlatFieldEnabled()) {
			FlatField f = configuration.getFlatField();
			f.writeHDFS(
				configuration.getOutputDir() + "/flatfield", 
				configuration);
		}

		int ret = 0;

		try {
			if (this.configuration.getAnalysisType() == XPCSConfig.ANALYSIS_TWOTIMES) {
				runTwoTimes();
			} else {
				runMultiTau();
			}
		} catch (Exception e) {
			ret = 1;
			e.printStackTrace();
		} finally {
			// Clean up
			//cleanup();	
		}

		running = false;

		return ret;
	}

	public void requestStop() {
		stopRequested = true;
	}

	public boolean isRunning() {
		return running;
	}

	private void runTwoTimes() throws Exception {
		FrameSum fsum = configuration.getFrameSum();
		fsum.writeHDFS(
			configuration.getOutputDir() + "/frameSum", 
			configuration);

		// PixelSum psum = configuration.getPixelSum()
		// psum.writeHDFS("input/pixelSum", configuration);

		SmoothedSG smsg = configuration.getSmoothedSG();
		smsg.writeHDFS(
			configuration.getOutputDir() + "/smoothedSG", 
			configuration);

		TwoTimesJobConf twotimes = new TwoTimesJobConf(configuration);
		twotimes.setJarByClass(Application.class);

		JobClient jobClient = new JobClient(configuration);
		RunningJob runningJob = jobClient.submitJob(twotimes);

		runAndMonitorJob(runningJob);

		if (stopRequested) {
			return;
		}

		ResultCollector collector = new ResultCollector(this.configuration);
		Thread statisticsThread = new Thread(collector);
		statisticsThread.start();

		statisticsThread.join();

		HDF5ResultHelper.writeTwoTime(configuration);
	}

	private void runMultiTau() throws Exception {
		G2JobConf g2JobConf = new G2JobConf(configuration);
		g2JobConf.setJarByClass(Application.class);

		JobClient jobClient = new JobClient(configuration);
		RunningJob runningJob = jobClient.submitJob(g2JobConf);

		runAndMonitorJob(runningJob);

		if (stopRequested) {
			return;
		}
		
		// Once the G2 job finish, we can collect some of the results and
		// do some local processing
		ResultCollector collector = new ResultCollector(this.configuration);
		Thread statisticsThread = new Thread(collector);
		statisticsThread.start();

		
		NormJobConf normJobConf = new NormJobConf(configuration);
		normJobConf.setJarByClass(Application.class);
		
		jobClient = new JobClient(configuration);
		runningJob = jobClient.submitJob(normJobConf);

		runAndMonitorJob(runningJob);

		if (stopRequested) {
			statisticsThread.join();
			return;
		}

		// We need to make sure that relevant results from the first
		// job are collected/written before recording results from the norm
		// We might be able to relax this restriction, depending on how final
		// results are gathered.
		statisticsThread.join();

		copyG2NormResults();

		mergeResults(configuration.getOutputDir() + File.separator + "norm",
				configuration.getOutputDir() + File.separator + "norm");

		String output = configuration.getOutputDir();
		HDF5ResultHelper.writeMultiTau(configuration, output);
	}

	private void runAndMonitorJob(RunningJob runningJob) {

		MonitorJob monitorjob = new MonitorJob(runningJob);
		Thread monitorJobThread = new Thread(monitorjob);
		monitorJobThread.start();

		while (monitorjob.isRunning()) {
			try {
				Thread.sleep(1000);
				if (stopRequested) {
					monitorjob.setShouldRun(false);
					monitorJobThread.join();

					runningJob.killJob();
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
	}

	private void mergeResults(String partResultsFolder, String resultsFolder) {
		File partResults = new File(partResultsFolder);
		File results = new File(resultsFolder + File.separator + "results");
		ArrayList<String> listResults = new ArrayList<String>();
		for (String file : partResults.list()) {
			if (file.substring(0, 5).equals("part-")) {
				listResults.add(file);
			}
		}

		Collections.sort(listResults);

		for (String a : listResults) {
			appendFile(partResults.getAbsolutePath() + File.separator + a,
					results.getAbsolutePath());
		}
	}

	private void appendFile(String from, String to) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(to, true));
			BufferedReader in = new BufferedReader(new FileReader(from));
			String str;
			while ((str = in.readLine()) != null) {
				out.write(str + "\n");
			}
			in.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void copyG2NormResults() throws IOException {
		Path normOutput = new Path(this.configuration.getOutputDir() + "/norm");
		FileSystem hdfs = FileSystem.get(this.configuration);
		hdfs.copyToLocalFile(normOutput, normOutput);
	}

	private void copy2TimesResults() throws IOException {
		Path outputPath = new Path(this.configuration.getOutputDir() + "/twotime");
		Path inputPath = new Path(this.configuration.getOutputDir() + "/g2");
		FileSystem hdfs = FileSystem.get(this.configuration);
		hdfs.copyToLocalFile(inputPath, outputPath);
	}

	private void cleanup() throws IOException {
		Path outputPath = new Path(this.configuration.getOutputDir());
		FileSystem hdfs = FileSystem.get(this.configuration);
		hdfs.delete(outputPath, true);
	}

	class MonitorJob implements Runnable {
		
		private RunningJob runningJob = null;
		private boolean isRunning = true;
		private boolean shouldRun = true;

		public MonitorJob(RunningJob job) {
			this.runningJob = job;
		}

		@Override
		public void run() {
			while (shouldRun) {

				try {
					if (! runningJob.isComplete()) {

						try {
							Thread.sleep(1000);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						shouldRun = false;
					}
				} catch (IOException io) {
					io.printStackTrace();
				}
			}

			System.out.println("Job done");
			isRunning = false;
		}

		public void setShouldRun(boolean flag) {
			shouldRun = flag;
		}

		public boolean isRunning() {
			return this.isRunning;
		}
	}

	class TheShutdownHook implements Runnable {
		private Application app = null;
		
		public TheShutdownHook(Application ap) {
			app = ap;
		}

		public void run() {
			System.out.println("Shutting down MapReduce job");

			app.requestStop();

			int attempts = 0;

			while (app.isRunning() && attempts < 5) {
				try {
					Thread.sleep(1000);
					attempts++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
