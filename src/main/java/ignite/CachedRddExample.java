package ignite;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteContext;
import org.apache.ignite.spark.IgniteRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * In this example we cache DataFrame by putting it's RDD into IgniteRDD,
 * and DataFrame schema is cached in IgniteCache. Basically data and schema are stored separately.
 */
public class CachedRddExample {

    public static void main(String[] args) throws Exception {
        new CachedRddExample().run();
    }

    private void run() throws Exception {

        // Create SparkContext and load data from Parquet
        final SparkConf sparkConf = new SparkConf()
                .setAppName("shared-rdd-example")
                .setMaster("local");
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);
        final DataFrame df = sqlContext.load("./data/nhtsa_sample.parquet");

        final IgniteContext igniteContext = new IgniteContext(sparkContext.sc(), "ignite/example-cache.xml", false);
        try {
            // Create cache for RDD schema
            final CacheConfiguration<String, StructType> schemaCacheConfig = makeSchemaCacheConfig("rdd-schema-cache");
            final IgniteCache<String, StructType> rddSchemaCache = igniteContext.ignite().createCache(schemaCacheConfig);

            for (int i = 0; i < 1000; i++) {
                final String schemaCacheName = "outpatient-schema";
                final long t0 = System.currentTimeMillis();
                rddSchemaCache.put(schemaCacheName, df.schema());
                System.out.println("DataFrame schema cached in " + (System.currentTimeMillis() - t0) + "ms");

                final String rddCacheName = "outpatient-rdd";
                final IgniteRDD igniteRDD = igniteContext.fromCache(rddCacheName);

                final long t = System.currentTimeMillis();
                df.rdd().count();
                System.out.println("DataFrame to RDD " + (System.currentTimeMillis() - t) + "ms");

                final long t1 = System.currentTimeMillis();
                igniteRDD.saveValues(df.rdd());
                System.out.println("RDD cached in " + (System.currentTimeMillis() - t1) + "ms");

                final long t2 = System.currentTimeMillis();
                final JavaRDD javaRDD = igniteRDD.toJavaRDD().map(a -> ((Tuple2)a)._2());
                System.out.println("RDD retrieved in " + (System.currentTimeMillis() - t2) + "ms");

                final long t3 = System.currentTimeMillis();
                final StructType rddSchema = rddSchemaCache.get(schemaCacheName);
                final DataFrame dataFrameFromCache = sqlContext.createDataFrame(javaRDD, rddSchema);
                System.out.println("DataFrame schema retrieved in " + (System.currentTimeMillis() - t3) + "ms");

                final long t4 = System.currentTimeMillis();
                dataFrameFromCache.collect();
                System.out.println("DataFrame collected in " + (System.currentTimeMillis() - t4) + "ms");

                igniteContext.ignite().cache(rddCacheName).clear();
            }
        } finally {
            igniteContext.ignite().close();
        }
    }

    private CacheConfiguration<String, StructType> makeSchemaCacheConfig(final String name) {
        return new CacheConfiguration<String, StructType>(name)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(0)
                .setAffinity(new RendezvousAffinityFunction(false, 1));
    }
}
