/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.filter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.collections.spatial.search.RadiusBound;
import com.metamx.collections.spatial.search.RectangularBound;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.SpatialDimFilter;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.SpatialDimensionSchema;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 */
@RunWith(Parameterized.class)
public class SpatialFilterBonusTest
{
  private static Interval DATA_INTERVAL = new Interval("2013-01-01/2013-01-07");

  private static AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("val", "val")
  };

  private static List<String> DIMS = Lists.newArrayList("dim", "dim.geo");

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final IncrementalIndex rtIndex = makeIncrementalIndex();
    final QueryableIndex mMappedTestIndex = makeQueryableIndex();
    final QueryableIndex mergedRealtimeIndex = makeMergedQueryableIndex();
    return Arrays.asList(
        new Object[][]{
            {
                new IncrementalIndexSegment(rtIndex)
            },
            {
                new QueryableIndexSegment(null, mMappedTestIndex)
            },
            {
                new QueryableIndexSegment(null, mergedRealtimeIndex)
            }
        }
    );
  }

  private static IncrementalIndex makeIncrementalIndex() throws IOException
  {
    IncrementalIndex theIndex = new IncrementalIndex(
        new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                            .withQueryGranularity(QueryGranularity.DAY)
                                            .withMetrics(METRIC_AGGS)
                                            .withSpatialDimensions(
                                                Arrays.asList(
                                                    new SpatialDimensionSchema(
                                                        "dim.geo",
                                                        Lists.<String>newArrayList()
                                                    )
                                                )
                                            ).build()
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-01").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-01").toString(),
                "dim", "foo",
                "dim.geo", "0.0,0.0",
                "val", 17l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-02").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-02").toString(),
                "dim", "foo",
                "dim.geo", "1.0,3.0",
                "val", 29l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-03").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-03").toString(),
                "dim", "foo",
                "dim.geo", "4.0,2.0",
                "val", 13l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-04").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-04").toString(),
                "dim", "foo",
                "dim.geo", "7.0,3.0",
                "val", 91l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "8.0,6.0",
                "val", 47l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-05").getMillis(),
            DIMS,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-05").toString(),
                "dim", "foo",
                "dim.geo", "_mmx.unknown",
                "val", 501l
            )
        )
    );

    // Add a bunch of random points
    Random rand = new Random();
    for (int i = 5; i < 5000; i++) {
      theIndex.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
                  "dim", "boo",
                  "dim.geo", String.format(
                  "%s,%s",
                  (float) (rand.nextFloat() * 10 + 10.0),
                  (float) (rand.nextFloat() * 10 + 10.0)
              ),
                  "val", i
              )
          )
      );
    }

    return theIndex;
  }

  private static QueryableIndex makeQueryableIndex() throws IOException
  {
    IncrementalIndex theIndex = makeIncrementalIndex();
    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();
    tmpFile.deleteOnExit();

    IndexMerger.persist(theIndex, tmpFile);
    return IndexIO.loadIndex(tmpFile);
  }

  private static QueryableIndex makeMergedQueryableIndex()
  {
    try {
      IncrementalIndex first = new IncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularity.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withSpatialDimensions(
                                                  Arrays.asList(
                                                      new SpatialDimensionSchema(
                                                          "dim.geo",
                                                          Lists.<String>newArrayList()
                                                      )
                                                  )
                                              ).build()
      );
      IncrementalIndex second = new IncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularity.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withSpatialDimensions(
                                                  Arrays.asList(
                                                      new SpatialDimensionSchema(
                                                          "dim.geo",
                                                          Lists.<String>newArrayList()
                                                      )
                                                  )
                                              ).build()
      );
      IncrementalIndex third = new IncrementalIndex(
          new IncrementalIndexSchema.Builder().withMinTimestamp(DATA_INTERVAL.getStartMillis())
                                              .withQueryGranularity(QueryGranularity.DAY)
                                              .withMetrics(METRIC_AGGS)
                                              .withSpatialDimensions(
                                                  Arrays.asList(
                                                      new SpatialDimensionSchema(
                                                          "dim.geo",
                                                          Lists.<String>newArrayList()
                                                      )
                                                  )
                                              ).build()
      );


      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-01").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-01").toString(),
                  "dim", "foo",
                  "dim.geo", "0.0,0.0",
                  "val", 17l
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-02").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-02").toString(),
                  "dim", "foo",
                  "dim.geo", "1.0,3.0",
                  "val", 29l
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-03").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-03").toString(),
                  "dim", "foo",
                  "dim.geo", "4.0,2.0",
                  "val", 13l
              )
          )
      );
      first.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "_mmx.unknown",
                  "val", 501l
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-04").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-04").toString(),
                  "dim", "foo",
                  "dim.geo", "7.0,3.0",
                  "val", 91l
              )
          )
      );
      second.add(
          new MapBasedInputRow(
              new DateTime("2013-01-05").getMillis(),
              DIMS,
              ImmutableMap.<String, Object>of(
                  "timestamp", new DateTime("2013-01-05").toString(),
                  "dim", "foo",
                  "dim.geo", "8.0,6.0",
                  "val", 47l
              )
          )
      );

      // Add a bunch of random points
      Random rand = new Random();
      for (int i = 5; i < 5000; i++) {
        third.add(
            new MapBasedInputRow(
                new DateTime("2013-01-01").getMillis(),
                DIMS,
                ImmutableMap.<String, Object>of(
                    "timestamp", new DateTime("2013-01-01").toString(),
                    "dim", "boo",
                    "dim.geo", String.format(
                    "%s,%s",
                    (float) (rand.nextFloat() * 10 + 10.0),
                    (float) (rand.nextFloat() * 10 + 10.0)
                ),
                    "val", i
                )
            )
        );
      }


      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();

      File firstFile = new File(tmpFile, "first");
      File secondFile = new File(tmpFile, "second");
      File thirdFile = new File(tmpFile, "third");
      File mergedFile = new File(tmpFile, "merged");

      firstFile.mkdirs();
      firstFile.deleteOnExit();
      secondFile.mkdirs();
      secondFile.deleteOnExit();
      thirdFile.mkdirs();
      thirdFile.deleteOnExit();
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      IndexMerger.persist(first, DATA_INTERVAL, firstFile);
      IndexMerger.persist(second, DATA_INTERVAL, secondFile);
      IndexMerger.persist(third, DATA_INTERVAL, thirdFile);

      QueryableIndex mergedRealtime = IndexIO.loadIndex(
          IndexMerger.mergeQueryableIndex(
              Arrays.asList(IndexIO.loadIndex(firstFile), IndexIO.loadIndex(secondFile), IndexIO.loadIndex(thirdFile)),
              METRIC_AGGS,
              mergedFile
          )
      );

      return mergedRealtime;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final Segment segment;

  public SpatialFilterBonusTest(Segment segment)
  {
    this.segment = segment;
  }

  @Test
  public void testSpatialQuery()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RadiusBound(new float[]{0.0f, 0.0f}, 5)
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 3L)
                            .put("val", 59l)
                            .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory();
      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testSpatialQueryMorePoints()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularity.DAY)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(
                                      new SpatialDimFilter(
                                          "dim.geo",
                                          new RectangularBound(new float[]{0.0f, 0.0f}, new float[]{9.0f, 9.0f})
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 17l)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-02T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 29l)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-03T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 13l)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-04T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 91l)
                            .build()
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-05T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("val", 47l)
                            .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory();
      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(segment),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}