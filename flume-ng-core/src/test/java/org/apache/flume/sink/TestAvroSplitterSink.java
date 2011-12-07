package org.apache.flume.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.AvroSplitterSink.AvroDetails;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSplitterSink {

	private static final Logger logger = LoggerFactory
			.getLogger(TestAvroSplitterSink.class);
	private static final String hostname1 = "localhost";
	private static final Integer port1 = 41414;
	private static final String hostname2 = "localhost";
	private static final Integer port2 = 41415;

	private AvroSplitterSink sink;
	private Channel channel;

	@Before
	public void setUp() {
		sink = new AvroSplitterSink();
		channel = new MemoryChannel();

		Context context = new Context();

		context.put("hostsWithPorts", "localhost:41414;localhost:41415");
		context.put("batch-size", "2");

		sink.setChannel(channel);

		Configurables.configure(sink, context);
		Configurables.configure(channel, context);
	}

	@Test
	public void testLifecycle() throws InterruptedException {
		Server servers[] = createServers();
		try {
			servers[0].start();
			servers[1].start();

			sink.start();
			Assert.assertTrue(LifecycleController.waitForOneOf(sink,
					LifecycleState.START_OR_ERROR, 5000));

			sink.stop();
			Assert.assertTrue(LifecycleController.waitForOneOf(sink,
					LifecycleState.STOP_OR_ERROR, 5000));
		} finally {
			servers[0].close();
			servers[1].close();
		}
	}

	@Test
	public void testProcess() throws InterruptedException,
			EventDeliveryException {
		Event event = EventBuilder.withBody("test event 1".getBytes(),
				new HashMap<String, String>());
		Server[] servers = createServers();

		servers[0].start();
		servers[1].start();

		sink.start();
		Assert.assertTrue(LifecycleController.waitForOneOf(sink,
				LifecycleState.START_OR_ERROR, 5000));

		Transaction transaction = channel.getTransaction();

		transaction.begin();
		for (int i = 0; i < 10; i++) {
			channel.put(event);
		}
		transaction.commit();
		transaction.close();

		for (int i = 0; i < 5; i++) {
			PollableSink.Status status = sink.process();
			Assert.assertEquals(PollableSink.Status.READY, status);
		}

		Assert.assertEquals(PollableSink.Status.BACKOFF, sink.process());

		sink.stop();
		Assert.assertTrue(LifecycleController.waitForOneOf(sink,
				LifecycleState.STOP_OR_ERROR, 5000));

		servers[0].close();
		servers[1].close();
	}

	private Server[] createServers() {
		Server server1 = new NettyServer(new SpecificResponder(
				AvroSourceProtocol.class, new MockAvroServer()),
				new InetSocketAddress(hostname1, port1));

		Server server2 = new NettyServer(new SpecificResponder(
				AvroSourceProtocol.class, new MockAvroServer()),
				new InetSocketAddress(hostname2, port2));

		return new Server[] { server1, server2 };
	}

	private static class MockAvroServer implements AvroSourceProtocol {

		@Override
		public Status append(AvroFlumeEvent event) throws AvroRemoteException {
			logger.debug("Received event:{}", event);
			return Status.OK;
		}

		@Override
		public Status appendBatch(List<AvroFlumeEvent> events)
				throws AvroRemoteException {

			logger.debug("Received event batch:{}", events);

			return Status.OK;
		}
	}

	@Test
	public void testGetHostsAndPortsromStringSuccess() {
		Map<String, List<Integer>> resultToValidate = AvroSplitterSink
				.getHostsAndPortsromString("localhost:41414;localhost:41415");
		assertEquals(1, resultToValidate.size());
		assertEquals(2, resultToValidate.get("localhost").size());
		assertTrue(Arrays.asList(41414, 41415).containsAll(
				resultToValidate.get("localhost")));

		resultToValidate = AvroSplitterSink
				.getHostsAndPortsromString("localhost:41414");
		assertEquals(1, resultToValidate.size());
		assertEquals(1, resultToValidate.get("localhost").size());
		assertTrue(Arrays.asList(41414).containsAll(
				resultToValidate.get("localhost")));

		resultToValidate = AvroSplitterSink
				.getHostsAndPortsromString("manynicesure-dx.eglbp.corp.yahoo.com:41414");
		assertEquals(1, resultToValidate.size());
		assertEquals(1,
				resultToValidate.get("manynicesure-dx.eglbp.corp.yahoo.com")
						.size());
		assertTrue(Arrays.asList(41414).containsAll(
				resultToValidate.get("manynicesure-dx.eglbp.corp.yahoo.com")));

		resultToValidate = AvroSplitterSink
				.getHostsAndPortsromString("10.66.78.243:41415");
		assertEquals(1, resultToValidate.size());
		assertEquals(1, resultToValidate.get("10.66.78.243").size());
		assertTrue(Arrays.asList(41415).containsAll(
				resultToValidate.get("10.66.78.243")));
	}

	@Test
	public void testGetHostToPortMappings() {
		List<AvroDetails> result = AvroSplitterSink
				.getHostToPortMappings("manynicesure-dx:41414;manynicesure-dx.eglbp.corp.yahoo.com:41415;10.66.78.243:41416;localhost:41417");
		assertEquals(4, result.size());
		for (AvroDetails anAtom : result) {
			if (anAtom.getHostName().trim().equals("manynicesure-dx"))
				assertEquals(41414, anAtom.getPortNumber().intValue());
			else if (anAtom.getHostName().trim()
					.equals("manynicesure-dx.eglbp.corp.yahoo.com"))
				assertEquals(41415, anAtom.getPortNumber().intValue());
			else if (anAtom.getHostName().trim().equals("10.66.78.243"))
				assertEquals(41416, anAtom.getPortNumber().intValue());
			else if (anAtom.getHostName().trim().equals("localhost"))
				assertEquals(41417, anAtom.getPortNumber().intValue());
			else
				fail("Did not expect a different host name!");
		}
	}
}
