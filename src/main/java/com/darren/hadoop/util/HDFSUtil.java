package com.darren.hadoop.util;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class HDFSUtil {
    private static final Logger LOG = Logger.getLogger(HDFSUtil.class);
    
    /**
     * read text file
     * 
     * @param conf
     * @param hdfsPath File path
     * @return List<String> collection of the text file lines
     * @throws Exception
     */
    public static List<String> readHDFSFile(Configuration conf, String hdfsPath) throws Exception {
        List<String> result = new ArrayList<String>();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        if (!fileSystem.exists(path)) {
            throw new FileNotFoundException("File Not found :" + path.toString());
        }

        FSDataInputStream inputStream = null;
        BufferedReader reader = null;

        try {
            FileStatus fileList[] = fileSystem.listStatus(path);
            for (FileStatus file : fileList) {
                if (file.isFile()) {
                    LOG.info(" *file.getPath()* " + file.getPath());
                    inputStream = fileSystem.open(file.getPath());
                    reader = new BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        result.add(line);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        } finally {
            reader.close();
            inputStream.close();
            fileSystem.close();
        }

        return result;
    }

    /**
     * Delete file
     * 
     * @param conf
     * @param hdfsPath File path
     * @throws IOException
     */
    public static void deleteHDFSFile(Configuration conf, String hdfsPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            LOG.info("HDFS deleted: " + path);
        }
        fileSystem.close();
    }

    /**
     * Get Campaign Id List
     * 
     * @param conf
     * @param campaignPath campaign's parent folder path
     * @param campaignNo the count of the campaign
     * @return List<String> the collection of campaign id
     * @throws IOException
     */
    public static List<String> getCampaignIdList(Configuration conf, String campaignPath, int campaignNo)
            throws IOException {
        List<String> campaignIdList = new ArrayList<String>();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(campaignPath);
        try {
            FileStatus fileList[] = fileSystem.listStatus(path);
            for (FileStatus file : fileList) {
                if (file.isDirectory() && file.getPath().getName().startsWith("campaignid=")) {
                    String folderName = file.getPath().getName();
                    campaignIdList.add(folderName.substring(folderName.indexOf("=") + 1));
                    // If campaignIdList.size() >= campaignNo, we don't collect the campaignId again
                    if (campaignNo != 0 && campaignIdList.size() >= campaignNo) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        } finally {
            fileSystem.close();
        }

        return campaignIdList;
    }
}
