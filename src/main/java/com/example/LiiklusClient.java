package com.example;

import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.processor.serialization.Message;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LiiklusClient {

	public static final String CONSUMER_GROUP = "liiklus-client";
	private static int i;

	public static void main(String[] args) {
		if (args.length < 3) {
			usage("Usage: LiiklusClient [--producer|--consumer] [liiklus-host:port] [stream-name] [producer-content-type (default text/plain)]");
		}
		String liiklusTarget = args[1];

		var channel = NettyChannelBuilder.forTarget(liiklusTarget)
				.directExecutor()
				.usePlaintext()
				.build();

		var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

		switch (args[0]) {
			case "--producer":
				Flux.<String>create(flux -> {
					while (true) {
						try {
							String payload = new BufferedReader(new InputStreamReader(System.in)).readLine();
							flux.next(payload);
						}
						catch (IOException e) {
							flux.error(e);
						}
					}
				}).concatMap(it -> stub.publish(
					PublishRequest.newBuilder()
							.setTopic(args[2])
							.setValue(stringAsMessage(it, args.length == 4 ? args[3] : "text/plain"))
							.setKey(ByteString.copyFromUtf8("irrelevant" + i++))
							.build()
			))
						.blockLast();

				break;

			case "--consumer":
				Flux.just(args[2])
						.flatMap(topic ->
								stub
										.subscribe(
												subscribeRequestFor(topic)
										)
										.flatMap(reply -> stub
												.receive(ReceiveRequest.newBuilder().setAssignment(reply.getAssignment()).build())
												.window(1)
												.concatMap(
														batch -> batch
																.map(ReceiveReply::getRecord)
																.doOnNext(rr -> System.out.format("%s: %s%n", topic, extractRiffMessage(rr).getPayload().toStringUtf8()))
																.delayUntil(record -> stub.ack(
																		AckRequest.newBuilder()
																				.setPartition(reply.getAssignment().getPartition())
																				.setGroup(CONSUMER_GROUP)
																				.setAssignment(reply.getAssignment())
																				.setOffset(record.getOffset())
																				.build()
																)),
														1
												)
										)
						).blockLast();
				break;
			default:
				usage("Usage: TestClient [--producer|--consumer] [liiklus-host] [stream-name]");
		}
	}

	private static Message extractRiffMessage(ReceiveReply.Record r) {
		try {
			return Message.parseFrom(r.getValue());
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}

	private static SubscribeRequest subscribeRequestFor(String topic) {
		return SubscribeRequest.newBuilder()
				.setTopic(topic)
				.setGroup(CONSUMER_GROUP)
				.setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
				.build();
	}

	private static ByteString stringAsMessage(String payload, String contentType) {
		return Message.newBuilder()
				.setPayload(ByteString.copyFromUtf8(payload))
				.setContentType(contentType)
				.build().toByteString();
	}

	private static void usage(String x) {
		System.err.println(x);
		System.exit(1);
	}
}
