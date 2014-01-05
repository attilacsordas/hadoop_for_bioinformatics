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

    public class AminoAcidPositionCounter extends Configured implements Tool {


        @SuppressWarnings("deprecation")
        public static class MyMap extends Mapper<LongWritable, Text,  Text, IntWritable> {

            private final  IntWritable one = new IntWritable(1);
            private final   Text onlyText = new Text();

            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


                String line = value.toString();
                //StringTokenizer tokenizer = new StringTokenizer(line);

                if (line.length() > 0) {
                    for (int i = 0; i < line.length(); i++) {

                        //System.out.println((i+1) + "\t" + line.length());

                        double result = (double)(i+1)/(double)(line.length());

                        //System.out.println(result);

                        String s = new StringBuilder().append(line.charAt(i)).append('_').append(i+1).append("_").append(roundTwoDecimals(result)).toString();
                        onlyText.set(s) ;
                        context.write(onlyText, one);
                    }
                }
            }

            private double roundTwoDecimals(double d) {
                DecimalFormat twoDForm = new DecimalFormat("#.##");
                return Double.valueOf(twoDForm.format(d));
            }

        }

        public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

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

/*
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		JobConf conf1 = new JobConf(NewPosCounter.class);
		conf1.setJobName("newposcounter");

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);

		conf1.setMapperClass(MyMap.class);
		// conf.setCombinerClass(Reduce.class);
		conf1.setReducerClass(MyReduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumReduceTasks(new Integer(10));

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

		JobClient.runJob(conf1);
	}
*/

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


        public int runJob(Configuration conf, String[] args) throws Exception {

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                System.err.println(arg);
            }
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


            Job job = new Job(conf, "newposcounter");
            conf = job.getConfiguration(); // NOTE JOB Copies the configuraton
            // This line runs the job on the cluster - omitting it runs the job locallty
            //conf.set("fs.default.name", "hdfs://" + HADOOP_MACHINE + ":" + HADOOP_PORT);

            job.setJarByClass(AminoAcidPositionCounter.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
/*        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/

            job.setMapperClass(MyMap.class);
            // conf.setCombinerClass(Reduce.class);
            job.setReducerClass(MyReduce.class);

        /*job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
*/
            job.setNumReduceTasks(new Integer(10));

/*
        job.setInputFormatClass(LineTextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
*/


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
        @Override
        public int run(final String[] args) throws Exception {
            Configuration conf = getConf();
            if (conf == null)
                conf = new Configuration();
            return runJob(conf, args);
        }


        public static final String HADOOP_MACHINE = "hadoop-master-03.ebi.ac.uk";
        public static final int HADOOP_PORT = 54310;

        private static void usage() {
            System.out.println("usage inputfile1 <inputfile2> <inputfile3> ... outputdirectory");
        }

        /**
         * Sample of use
         * args might be /user/slewis/hadoop/test/books/pg135.txt /user/slewis/hadoop/test/output1
         *
         * @param args
         * @throws Exception
         */
        public static void main(String[] args) throws Exception {
            if (args.length < 2) {
                usage();
                return;
            }

            new AminoAcidPositionCounter().run(args);
        }

    }





