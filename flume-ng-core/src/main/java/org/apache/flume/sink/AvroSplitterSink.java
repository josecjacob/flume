package org.apache.flume.sink;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link Sink} implementation that can send events to an Avro server (such as
 * Flume's <tt>AvroSource</tt>).
 * </p>
 * <p>
 * This sink forms one half of Flume's tiered collection support. Events sent to
 * this sink are turned into {@link AvroFlumeEvent}s and sent to the configured
 * hostname / port pair using Avro's {@link NettyTransceiver}. The intent is
 * that the destination is an instance of Flume's <tt>AvroSource</tt> which
 * allows Flume to nodes to forward to other Flume nodes forming a tiered
 * collection infrastructure. Of course, nothing prevents one from using this
 * sink to speak to other custom built infrastructure that implements the same
 * Avro protocol (specifically {@link AvroSourceProtocol}).
 * </p>
 * <p>
 * Events are taken from the configured {@link Channel} in batches of the
 * configured <tt>batch-size</tt>. The batch size has no theoretical limits
 * although all events in the batch <b>must</b> fit in memory. Generally, larger
 * batches are far more efficient, but introduce a slight delay (measured in
 * millis) in delivery. The batch behavior is such that underruns (i.e. batches
 * smaller than the configured batch size) are possible. This is a compromise
 * made to maintain low latency of event delivery. If the channel returns a null
 * event, meaning it is empty, the batch is immediately sent, regardless of
 * size. Batch underruns are tracked in the metrics. Empty batches do not incur
 * an RPC roundtrip.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>hostname</tt></td>
 * <td>The hostname to which events should be sent.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which events should be sent on <tt>hostname</tt>.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>batch-size</tt></td>
 * <td>The maximum number of events to send per RPC.</td>
 * <td>events / int</td>
 * <td>100</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSplitterSink extends AbstractSink implements PollableSink,
		Configurable {

	/**
	 * @author chira
	 * 
	 */
	protected static final class AvroDetails {
		private String hostName;
		private Integer portNumber;
		private AvroSourceProtocol client;
		private Transceiver transceiver;

		/**
		 * @param transceiver
		 * @param client
		 */
		public AvroDetails(String hostName, Integer portNumber) {
			this.hostName = hostName;
			this.portNumber = portNumber;
		}

		public void initialize() {
			Transceiver transceiver = null;
			try {
				transceiver = new NettyTransceiver(new InetSocketAddress(
						this.getHostName(), this.getPortNumber()));
				AvroSourceProtocol client = SpecificRequestor.getClient(
						AvroSourceProtocol.class, transceiver);
				this.setTransceiver(transceiver);
				this.setClient(client);
			} catch (Exception e) {
				logger.error("Unable to create avro client using hostname:"
						+ this.getHostName() + " port:" + this.getPortNumber()
						+ ". Exception follows.", e);

				/* Try to prevent leaking resources. */
				if (transceiver != null) {
					try {
						transceiver.close();
					} catch (IOException e1) {
						logger.error(
								"Attempt to clean up avro tranceiver after client error failed. Exception follows.",
								e1);
					}
				}

				/* FIXME: Mark ourselves as failed. */
				return;
			}
		}

		/**
		 * @return the client
		 */
		public AvroSourceProtocol getClient() {
			return client;
		}

		/**
		 * @param client
		 *            the client to set
		 */
		public void setClient(AvroSourceProtocol client) {
			this.client = client;
		}

		/**
		 * @return the transceiver
		 */
		public Transceiver getTransceiver() {
			return transceiver;
		}

		/**
		 * @param transceiver
		 *            the transceiver to set
		 */
		public void setTransceiver(Transceiver transceiver) {
			this.transceiver = transceiver;
		}

		/**
		 * @return the portNumber
		 */
		public Integer getPortNumber() {
			return portNumber;
		}

		/**
		 * @return the hostName
		 */
		public String getHostName() {
			return hostName;
		}
	}

	private static final Logger logger = LoggerFactory
			.getLogger(AvroSplitterSink.class);
	private static final Integer defaultBatchSize = 100;

	private List<AvroDetails> avroDetailsList;
	private Integer batchSize;

	private CounterGroup counterGroup;

	public AvroSplitterSink() {
		counterGroup = new CounterGroup();
	}

	@Override
	public void configure(Context context) {
		avroDetailsList = getHostToPortMappings(context
				.getString("hostsWithPorts"));
		batchSize = Integer.parseInt(context.get("batch-size", String.class));

		if (batchSize == null) {
			batchSize = defaultBatchSize;
		}
	}

	public static List<AvroDetails> getHostToPortMappings(String hostsWithPorts) {
		logger.debug("The hostname string we will parse is: " + hostsWithPorts);
		List<AvroDetails> result = new ArrayList<AvroSplitterSink.AvroDetails>();
		Map<String, List<Integer>> hostToPortMappings = getHostsAndPortsromString(hostsWithPorts);
		for (String hostName : hostToPortMappings.keySet()) {
			for (Integer aPort : hostToPortMappings.get(hostName))
				result.add(new AvroDetails(hostName, aPort));
			logger.debug("Received a host name: " + hostName
					+ " against the port string: "
					+ hostToPortMappings.get(hostName));
		}

		return result;
	}

	public static Map<String, List<Integer>> getHostsAndPortsromString(
			String hostsWithPorts) {

		Map<String, List<Integer>> hostToPortMapping = new HashMap<String, List<Integer>>();
		List<String> hostsWithPortsList = Arrays.asList(hostsWithPorts
				.split(";"));

		// add each of them into the list ...
		for (String hostPortMap : hostsWithPortsList) {
			String[] hostAndPortArray = hostPortMap.split(":");
			Preconditions.checkState(hostAndPortArray.length == 2);
			if (hostToPortMapping.get(hostAndPortArray[0]) != null)
				hostToPortMapping.get(hostAndPortArray[0]).add(
						Integer.parseInt(hostAndPortArray[1]));
			else
				hostToPortMapping.put(
						hostAndPortArray[0],
						new ArrayList<Integer>(Arrays.asList(Integer
								.parseInt(hostAndPortArray[1]))));
		}
		return hostToPortMapping;
	}

	@Override
	public void start() {
		logger.info("Avro sink starting");

		for (AvroDetails avroDetail : avroDetailsList) {
			avroDetail.initialize();
		}

		super.start();

		logger.debug("Avro sink started");
	}

	@Override
	public void stop() {
		logger.info("Avro sink stopping");

		try {
			for (AvroDetails anElement : avroDetailsList) {
				anElement.getTransceiver().close();
			}
		} catch (IOException e) {
			logger.error(
					"Unable to shut down avro tranceiver - Possible resource leak!",
					e);
		}

		super.stop();

		logger.debug("Avro sink stopped. Metrics:{}", counterGroup);
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		try {
			transaction.begin();

			List<AvroFlumeEvent> batch = new LinkedList<AvroFlumeEvent>();

			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();

				if (event == null) {
					counterGroup.incrementAndGet("batch.underflow");
					break;
				}

				AvroFlumeEvent avroEvent = new AvroFlumeEvent();

				avroEvent.body = ByteBuffer.wrap(event.getBody());
				avroEvent.headers = new HashMap<CharSequence, CharSequence>();

				for (Entry<String, String> entry : event.getHeaders()
						.entrySet()) {
					avroEvent.headers.put(entry.getKey(), entry.getValue());
				}

				batch.add(avroEvent);
			}

			if (batch.isEmpty()) {
				counterGroup.incrementAndGet("batch.empty");
				status = Status.BACKOFF;
			} else {
				for (AvroDetails anElement : avroDetailsList) {
					if (!anElement.getClient().appendBatch(batch)
							.equals(org.apache.flume.source.avro.Status.OK)) {
						throw new AvroRemoteException(
								"RPC communication returned FAILED");
					}
				}
			}

			transaction.commit();
			counterGroup.incrementAndGet("batch.success");
		} catch (ChannelException e) {
			transaction.rollback();
			logger.error(
					"Unable to get event from channel. Exception follows.", e);
			status = Status.BACKOFF;
		} catch (AvroRemoteException e) {
			transaction.rollback();
			logger.error("Unable to send event batch. Exception follows.", e);
			status = Status.BACKOFF;
		} finally {
			transaction.close();
		}

		return status;
	}

}
