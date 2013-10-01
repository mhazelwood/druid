/**
 *
 */
package com.attensity.druid;


import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.common.logger.Logger;

import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;

/**
 * @author mhazelwood
 */
@JsonTypeName("pipeline")
public class PipelineFirehoseFactory implements FirehoseFactory {
	private static final Logger log = new Logger(PipelineFirehoseFactory.class);

	/**
	 * max events to receive, -1 is infinite, 0 means nothing is delivered; use
	 * this to prevent infinite space consumption or to prevent Attensity
	 * Pipeline from being overloaded at an inconvenient time or to see what
	 * happens when Pipeline stops delivering values, or to have hasMore()
	 * return false.
	 */
	private final int maxEventCount;

	/**
	 * maximum number of minutes to fetch Pipeline articles. Use this to limit
	 * run time. If zero or less, no time limit for run.
	 */
	private final int maxRunMinutes;

	/**
	 * URL to connect to for the web socket feed
	 */
	private final String webSocketUrl;

	@JsonCreator
	public PipelineFirehoseFactory(
			@JsonProperty("maxEventCount") Integer maxEventCount,
			@JsonProperty("maxRunMinutes") Integer maxRunMinutes,
			@JsonProperty("webSocketUrl") String webSocketUrl) {
		this.maxEventCount = maxEventCount;
		this.maxRunMinutes = maxRunMinutes;
		this.webSocketUrl = webSocketUrl;
		log.info("maxEventCount="
				+ ((maxEventCount <= 0) ? "no limit" : maxEventCount));
		log.info("maxRunMinutes="
				+ ((maxRunMinutes <= 0) ? "no limit" : maxRunMinutes));
		log.info("webSocketUrl="+webSocketUrl);
	}

	@Override
	public Firehose connect() throws IOException {
		log.info("connecting...");
		PipelineFirehose firehose = new PipelineFirehose(maxEventCount, maxRunMinutes, webSocketUrl);
		firehose.connect();
		return firehose;
	}

}
