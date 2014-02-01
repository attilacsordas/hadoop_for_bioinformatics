package bioinformatics.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by attilacsordas on 05/01/2014.
 */

public class AminoAcidCounter extends Configured implements Tool {

    // mapper subclass

    @SuppressWarnings("deprecation")
    public static class MyMap extends Mapper<LongWritable, Text,  Text, IntWritable> {

        // avoiding object creation and overhead in the map method, reusing these objects
        private final  IntWritable one = new IntWritable(1);
        private final   Text onlyText = new Text();

        // map method is called for every key-value input pairs, Context is used to write intermediate data and job configuration, setup, cleanup
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // retrieving the String on the 'value' Text object
            String line = value.toString();
            //StringTokenizer tokenizer = new StringTokenizer(line);

            if (line.length() > 0) {

                // looping through characters (amino acids) in the string (the peptide)
                for (int i = 0; i < line.length(); i++) {

                    String n = new StringBuilder().append(line.charAt(i)).toString();
                    onlyText.set(n);
                    context.write(onlyText, one);

                }
            }
        }

    }

    // reducer subclass

    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


        // iterator over values belonging to 1 key, called once for each key
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            IntWritable f = new IntWritable(sum);
            context.write(key, f);

            System.out.println(key + " " + f.toString());
        }
    }

    // utility for rapid development: do not need to manually delete output directory every time the code runs

    /**
     * kill a directory and all contents
     *
     * @param src
     * @param fs
     * @return
     */
    public static boolean expunge(Path src, FileSystem fs) {


        try {
            if (!fs.exists(src))
                return true;
            // break these out
            if (fs.getFileStatus(src).isDir()) {
                boolean doneOK = fs.delete(src, true);
                doneOK = !fs.exists(src);
                return doneOK;
            }
            if (fs.isFile(src)) {
                boolean doneOK = fs.delete(src, false);
                return doneOK;
            }
            throw new IllegalStateException("should be file of directory if it exists");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // driver part: configures the job and runs it in local and/or submits it to the cluster

    public int runJob(Configuration conf, String[] args) throws Exception {

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            System.err.println(arg);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // setting configuration options for the job: mapper and reducer classes, input and output directories, data formats to be used
        Job job = new Job(conf, "aminoacidcounter");
        conf = job.getConfiguration(); // NOTE JOB Copies the configuraton
        // This line runs the job on the cluster - omitting it runs the job locally
        //conf.set("fs.default.name", "hdfs://" + HADOOP_MACHINE + ":" + HADOOP_PORT);

        job.setJarByClass(AminoAcidCounter.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
/*        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/


        job.setMapperClass(MyMap.class);
        // conf.setCombinerClass(Reduce.class);
        job.setReducerClass(MyReduce.class);

        // TextInputFormat is the default, no need to explicitly specify
        /*job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); */

        // set reducers to scale up to the nodes on the cluster
        //job.setNumReduceTasks(new Integer(10));


        if (otherArgs.length > 1) {
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        }

        // you must pass the output directory as the last argument
        String athString = otherArgs[otherArgs.length - 1];
        //       File out = new File(athString);
//        if (out.exists()) {
//            FileUtilities.expungeDirectory(out);
//            out.delete();
//        }

        Path outputDir = new Path(athString);

        FileSystem fileSystem = outputDir.getFileSystem(conf);
        expunge(outputDir, fileSystem);    // make sure this does not exist
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outputDir);

        //  Blocks"(waits"for"the"job"to"complete"before"continuing)", other method is job.submit()  Does"not"block"(driver"code"continues"as"the"job"is"running)
        boolean ans = job.waitForCompletion(true);
        int ret = ans ? 0 : 1;
        return ret;
    }


    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
   //@Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        if (conf == null)
            conf = new Configuration();
        return runJob(conf, args);
    }

    private static void usage() {
        System.out.println("usage: please specify inputdirectory ... outputdirectory as arguments");
    }

    /**
     * Sample of use
     * args might be /input /output
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }

        new AminoAcidCounter().run(args);
    }

}






