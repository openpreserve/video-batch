VideoBatch
===========

This MapReduce application processes video files on Hadoop clusters. It implements input formats and record readers by wrapping [Xuggler][http://xuggle.com/].

Instructions
------------

* Download and build Xuggler 5.4 following the installation instruction [here][http://xuggle.com/xuggler/build]. You will have to do this on each node of your cluster.
* Link or copy built xuggle-\*.jar to lib directory of the Hadoop installation on each cluster node.
* Download or clone VideoBatch source code and run `mvn install`.
* Copy a video file to HDFS. The video file should be encoded with keyframes at reasonable intervals. 
* Depending on your video file size and encoding, choose an appropriate splitsize. Through the splitsize you can determine approximately how many frames per task are going to be processed.
* Run VideoBatch using SimpleVideoBatch as main class:
    hadoop jar target/VideoBatch-1.0-SNAPSHOT.jar at.ac.ait.dme.video_batch.SimpleVideoBatch </path/to/video/file/on/hdfs> <splitsize>
* As a proof of concept the default DummyVideoMapper just writes out the frames to image files in `/tmp/output/`


