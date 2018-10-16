package com.dse.se.dao.shell;

import com.dse.se.dao.IStreamingJobDAO;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by michaelraney on 4/7/18.
 */


@Component
public class ShellDAO implements IStreamingJobDAO {

    public static void main (String[] args) throws Exception{
       new ShellDAO().startStreaming("/Users/michaelraney/dev/git/datastax-approximation-examples/dse-sketching-demo/dse-sketching-streaming-job/script/streaming-job.sh",
               "/Users/michaelraney/dse/dse-5.1.2/bin", "/Users/michaelraney/dev/git/datastax-approximation-examples/dse-sketching-demo/dse-sketching-streaming-job", "tH7vVFQIncx3tj0NIuo8vqOUC", "evnRBoQbW24Wb8IpHRhkZARHUzO83M4YQ6KxqZqenrzUYUBGcg", "58465931-2gN1Ofp7Smyg0A0h8EWsbTDAd39LgLfO7WAnfGFeH", "v7cEbLGw0vwkuuGsZsabsMWmZMjCz7C2Nev9TXRa9xW8I");
    }

    public void startStreaming(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws Exception {


        //RightScale
        //TODO:Extract this to external config
        String scriptFilePath = "/tmp/datastax-sketch-examples/dse-sketching-demo/dse-sketching-streaming-job/script/streaming-job.sh";
        String executionDirectory = "/tmp/datastax-sketch-examples";
        String sparkJobJarDir = "/tmp/datastax-sketch-examples/dse-sketching-demo/dse-sketching-streaming-job/target";

/*
        String scriptFilePath =      "/Users/michaelraney/dev/git/datastax-approximation-examples/dse-sketching-demo/dse-sketching-streaming-job/script/streaming-job.sh";
        String executionDirectory =  "/Users/michaelraney/dse/dse-5.1.2/bin";
        String sparkJobJarDir =      "/Users/michaelraney/dev/git/datastax-approximation-examples/dse-sketching-demo/dse-sketching-streaming-job/target";
*/

        startStreaming(scriptFilePath, executionDirectory, sparkJobJarDir, consumerKey, consumerSecret, accessToken, accessTokenSecret);

    }
    private void startStreaming(String scriptFilePath, String executionDirectory, String sparkJobJarDir, String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws Exception{


        File sf = new File(scriptFilePath);
        File ed = new File(executionDirectory);
        File sd = new File(sparkJobJarDir);

        System.out.println(scriptFilePath + " exists:" + sf.exists() );
        System.out.println(executionDirectory + " exists:" + ed.exists());
        System.out.println(sparkJobJarDir + " exists:" + sd.exists());


        ProcessBuilder pb = new ProcessBuilder(scriptFilePath, sparkJobJarDir,
                consumerKey, consumerSecret, accessToken, accessTokenSecret);


        //Map<String, String> env = pb.environment();
        //env.put("VAR1", "myValue");
        //env.remove("OTHERVAR");
        //env.put("VAR2", env.get("VAR1") + "suffix");
        pb.directory(ed);
        Process p = pb.start();
/*
        BufferedReader br=new BufferedReader(
                new InputStreamReader(
                        p.getInputStream()));
        String line;
        while((line=br.readLine())!=null){
            System.out.println(line);
        }*/
        p.waitFor(15L, TimeUnit.SECONDS);

    }
}
