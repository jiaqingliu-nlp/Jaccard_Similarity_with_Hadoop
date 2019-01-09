package ruc.df7.r320;

import java.io.*;
import java.lang.reflect.Field;

import java.net.*;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JacSimCalc {
	private static final Pattern SPACE_REG = Pattern.compile("^\\d+\\s+");
	private static int k = 3;
	private static double threshold = 0.3;
	private static String prefix = "";
	private static String suffix = "";

	/**
	 * 如果已经存在factory，则加一个装饰器，将原来的factory和用来读取hdfs的factory都封装进去，按需使用
	 *
	 * @param fsUrlStreamHandlerFactory
	 * @throws Exception
	 */
	private static void registerFactory(final FsUrlStreamHandlerFactory fsUrlStreamHandlerFactory) throws Exception {
		// log.info("registerFactory : " +
		// fsUrlStreamHandlerFactory.getClass().getName());
		final Field factoryField = URL.class.getDeclaredField("factory");
		factoryField.setAccessible(true);
		final Field lockField = URL.class.getDeclaredField("streamHandlerLock");
		lockField.setAccessible(true);
		// use same lock as in java.net.URL.setURLStreamHandlerFactory
		synchronized (lockField.get(null)) {
			final URLStreamHandlerFactory originalUrlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField
					.get(null);
			// Reset the value to prevent Error due to a factory already defined
			factoryField.set(null, null);
			URL.setURLStreamHandlerFactory(protocol -> {
				if (protocol.equals("hdfs")) {
					return fsUrlStreamHandlerFactory.createURLStreamHandler(protocol);
				} else {
					return originalUrlStreamHandlerFactory.createURLStreamHandler(protocol);
				}
			});
		}
	}

	public static Double calculateJaccardSimilarity(final CharSequence left, final CharSequence right) {
		final int leftLength = left.length();
		final int rightLength = right.length();
		if (leftLength == 0 || rightLength == 0) {
			return 0d;
		}
		final Set<Character> leftSet = new HashSet<>();
		for (int i = 0; i < leftLength; i++) {
			leftSet.add(left.charAt(i));
		}
		final Set<Character> rightSet = new HashSet<>();
		for (int i = 0; i < rightLength; i++) {
			rightSet.add(right.charAt(i));
		}
		final Set<Character> unionSet = new HashSet<>(leftSet);
		unionSet.addAll(rightSet);
		final int intersectionSize = leftSet.size() + rightSet.size() - unionSet.size();
		return 1.0d * intersectionSize / unionSet.size();
	}

	public static final Map<String, Integer> getProfile(final String string) {
		HashMap<String, Integer> shingles = new HashMap<String, Integer>();

		for (int i = 0; i < (string.length() - k + 1); i++) {
			String shingle = string.substring(i, i + k);
			Integer old = shingles.get(shingle);
			if (old != null) {
				shingles.put(shingle, old + 1);
			} else {
				shingles.put(shingle, 1);
			}
		}

		return Collections.unmodifiableMap(shingles);
	}

	public static Double calculateJaccardSimilarityAlt(final String s1, final String s2) {
		if (s1 == null) {
			throw new NullPointerException("s1 must not be null");
		}

		if (s2 == null) {
			throw new NullPointerException("s2 must not be null");
		}

		if (s1.equals(s2)) {
			return 1.0;
		}

		Map<String, Integer> profile1 = getProfile(s1);
		Map<String, Integer> profile2 = getProfile(s2);

		Set<String> union = new HashSet<String>();
		union.addAll(profile1.keySet());
		union.addAll(profile2.keySet());

		int inter = profile1.keySet().size() + profile2.keySet().size() - union.size();

		return (1.0 * inter / union.size());
	}

	public static class PairGenerator extends Mapper<Object, Text, Text, DoubleWritable> {
		private static Vector<String> File2Map = new Vector<String>();
		private String word = new String();
		private BufferedReader brReader;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			URI[] cacheFilesLocal = context.getCacheFiles();

			for (URI eachPath : cacheFilesLocal) {
				loadFile2(eachPath, context);
			}
		}

		private void loadFile2(URI filePath, Context context) throws IOException {
			String strLineRead = "";
			InputStream in = new URL(filePath.toString()).openStream();

			try {
				brReader = new BufferedReader(new InputStreamReader(in));
				while ((strLineRead = brReader.readLine()) != null) {
					String trim = SPACE_REG.matcher(strLineRead).replaceAll("");
					File2Map.add(trim.toLowerCase());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (brReader != null) {
					brReader.close();
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			word = SPACE_REG.matcher(value.toString()).replaceAll("").toLowerCase();
			for (String s : File2Map) {
				Double v = calculateJaccardSimilarityAlt(prefix + word + suffix, prefix + s + suffix);
				if (v >= threshold && v <= 1) {
					context.write(new Text("<" + word + "," + s + ">"), new DoubleWritable(v));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		registerFactory(new FsUrlStreamHandlerFactory());

		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();

		if (remainingArgs.length != 5) {
			System.err.println("Usage: JacSimCalc <R> <S> <threshold> <N> <output>");
			System.exit(2);
		}

		FileSystem fs = FileSystem.get(conf);
		Path p0 = new Path(args[0]);
		Path p1 = new Path(fs.getUri().toString() + args[1]);

		threshold = Double.parseDouble(args[2]);
		k = Integer.parseInt(args[3]);
		Path p2 = new Path(args[4]);

		if (k < 0) {
			System.err.println("Length of gram can not < 0!");
			System.exit(3);
		}

		if (fs.exists(p2)) {
			fs.delete(p2, true);
		}

		StringBuffer tempPrefix = new StringBuffer();
		StringBuffer tempSuffix = new StringBuffer();

		for (int i = 0; i < k - 1; i++) {
			tempPrefix.append("#");
			tempSuffix.append("$");
		}

		prefix = tempPrefix.toString();
		suffix = tempSuffix.toString();

		Job job = Job.getInstance(conf, "JaccardSimilarityCalc");

		job.setJarByClass(JacSimCalc.class);
		job.addCacheFile(p1.toUri());
		job.setMapperClass(PairGenerator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, p0);
		FileOutputFormat.setOutputPath(job, p2);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}