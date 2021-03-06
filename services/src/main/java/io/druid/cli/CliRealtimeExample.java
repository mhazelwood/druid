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

package io.druid.cli;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.metamx.common.logger.Logger;
import druid.examples.flights.FlightsFirehoseFactory;
import druid.examples.rand.RandomFirehoseFactory;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import druid.examples.web.WebFirehoseFactory;
import io.airlift.command.Command;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.client.ServerView;
import io.druid.guice.NoopSegmentPublisherProvider;
import io.druid.guice.RealtimeModule;
import io.druid.indexing.common.index.EventReceiverFirehoseFactory;
import io.druid.indexing.common.index.StaticS3FirehoseFactory;
import io.druid.initialization.DruidModule;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.IrcFirehoseFactory;
import io.druid.segment.realtime.firehose.KafkaFirehoseFactory;
import io.druid.segment.realtime.firehose.RabbitMQFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 */
@Command(
    name = "realtime",
    description = "Runs a standalone realtime node for examples, see https://github.com/metamx/druid/wiki/Realtime for a description"
)
public class CliRealtimeExample extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliRealtimeExample()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new RealtimeModule(),
        new DruidModule()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(SegmentPublisher.class).toProvider(NoopSegmentPublisherProvider.class);
            binder.bind(DataSegmentPusher.class).to(NoopDataSegmentPusher.class);
            binder.bind(DataSegmentAnnouncer.class).to(NoopDataSegmentAnnouncer.class);
            binder.bind(InventoryView.class).to(NoopInventoryView.class);
            binder.bind(ServerView.class).to(NoopServerView.class);
          }

          @Override
          public List<Module> getJacksonModules()
          {
            return Arrays.<Module>asList(
                new SimpleModule("RealtimeExampleModule")
                    .registerSubtypes(
                        new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
                        new NamedType(FlightsFirehoseFactory.class, "flights"),
                        new NamedType(RandomFirehoseFactory.class, "rand"),
                        new NamedType(WebFirehoseFactory.class, "webstream"),
                        new NamedType(KafkaFirehoseFactory.class, "kafka"),
                        new NamedType(RabbitMQFirehoseFactory.class, "rabbitmq"),
                        new NamedType(ClippedFirehoseFactory.class, "clipped"),
                        new NamedType(TimedShutoffFirehoseFactory.class, "timed"),
                        new NamedType(IrcFirehoseFactory.class, "irc"),
                        new NamedType(StaticS3FirehoseFactory.class, "s3"),
                        new NamedType(EventReceiverFirehoseFactory.class, "receiver")
                    )
            );
          }
        }
    );
  }

  private static class NoopServerView implements ServerView
  {
    @Override
    public void registerServerCallback(
        Executor exec, ServerCallback callback
    )
    {
      // do nothing
    }

    @Override
    public void registerSegmentCallback(
        Executor exec, SegmentCallback callback
    )
    {
      // do nothing
    }
  }

  private static class NoopInventoryView implements InventoryView
  {
    @Override
    public DruidServer getInventoryValue(String string)
    {
      return null;
    }

    @Override
    public Iterable<DruidServer> getInventory()
    {
      return ImmutableList.of();
    }
  }

  private static class NoopDataSegmentPusher implements DataSegmentPusher
  {
    @Override
    public DataSegment push(File file, DataSegment segment) throws IOException
    {
      return segment;
    }
  }

  private static class NoopDataSegmentAnnouncer implements DataSegmentAnnouncer
  {
    @Override
    public void announceSegment(DataSegment segment) throws IOException
    {
      // do nothing
    }

    @Override
    public void unannounceSegment(DataSegment segment) throws IOException
    {
      // do nothing
    }

    @Override
    public void announceSegments(Iterable<DataSegment> segments) throws IOException
    {
      // do nothing
    }

    @Override
    public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
    {
      // do nothing
    }
  }
}
