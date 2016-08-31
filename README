
The instruction for how to compile the code with "Maven" are in the INSTALL file. This document describes
major changes in each version of the code. The feature list here is not comprehensive, please consult the
confluence page at https://confluence.aps.anl.gov/display/XPCS/XPCS+Analysis for more details.

Version 0.4.2
_____________

1. Added the pipeline launch scripts along with the required files (jars, scripts etc). This provides
a complete launch package for XPCS analysis.The pipeline version was taken from the Tag 0.1-20140311 
(See https://subversion.xray.aps.anl.gov/pipeline/java/tags/0.1-20140311/). 


Version 0.4.1
______________

The version 0.4.1 is the most complete version that is in production since late 2013 at Sector 8. Here is a brief
list of features in this version:

- The complete multitau analysis of data in all three formats :Sparse, Non-
Sparse, and Kinetics. 

- Process the IMM file stored directly in the Hadoop
file system (HDFS). 

- Read the job configuration parameters from the HDF5
file (See the confluence page for more information on how the hdf5
parameters are interpreted along with their type:
https://confluence.aps.anl.gov/display/XPCS/Job+Parameters) 

- The final results produced by the MapReduce are converted to a format that can be
written as a HDF5 dataset. (See the confluene page
https://confluence.aps.anl.gov/display/XPCS/HDF5+Output+Format for more
details on the output format)

- This version provides a partial fix for the concurrency issues 
involving processing of input parameters from HDF5 file.
Our code reads the    input parameters from the fixed location within the HDF5
file. As of this version it is the dataset named '/xpcs'. Before this version
the '/xpcs'   would be read twice: first, for configuring the job parameters
(input, qmap etc), second, for determining the location of the output dataset
in the same   HDF5 file. The problem arises when an external    program (e.g.
Suresh's Matlab) modifies the value under '/xpcs' in between these two reads.
We fixed it by reading all the information needed including the   location of
the output folder. However, this is a problem that require a bit re-
engineering in particular when it will come to submitting multiple jobs using
the pipeline.
