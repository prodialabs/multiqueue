import { assertEquals } from "https://deno.land/std@0.181.0/testing/asserts.ts";
import { connect } from "./deps.ts";
import { createMultiQueue } from "./main.ts";

const redis = await connect({
	hostname: "127.0.0.1",
});

await redis.flushall();

await (async () => {
	console.log("Testing basic functionality...");

	const myQueue = "images-to-be-processed";
	type Queue = typeof myQueue;

	type Job = {
		id: string;
		numbers: number[];
	};

	const mq = createMultiQueue<Queue, Job>({
		redis,
		prefix: "multiqueue-test-1",
		retryAfter: 500,
	});

	assertEquals(
		await mq.pop(myQueue),
		undefined,
		"pop() must return undefined when no jobs",
	);

	const firstJob: Job = { id: "unique-id-here", numbers: [Math.random()] };
	const secondJob: Job = { id: "unique-id-here-2", numbers: [Math.random()] };

	await mq.push(myQueue, secondJob, 500);
	await mq.push(myQueue, firstJob, 250);

	assertEquals(
		await mq.pop(myQueue),
		firstJob,
		"pop() must return correct job",
	);

	assertEquals(
		await mq.pop(myQueue),
		secondJob,
		"pop() must return correct job",
	);
})();

await (async () => {
	console.log("Testing popAny functionality...");

	const myQueue1 = "queue1";
	const myQueue2 = "queue2";
	type Queue = typeof myQueue1 | typeof myQueue2;

	type Job = {
		id: string;
		numbers: number[];
	};

	const mq = createMultiQueue<Queue, Job>({
		redis,
		prefix: "multiqueue-popany:",
		retryAfter: 500,
	});

	assertEquals(
		await mq.popAny(),
		undefined,
		"popAny() must return undefined when no jobs",
	);

	const firstJob: Job = {
		id: "unique-id-here",
		numbers: [Math.random()],
	};

	const secondJob: Job = {
		id: "unique-id-here-2",
		numbers: [Math.random()],
	};

	await mq.push(myQueue2, secondJob, 500);
	await mq.push(myQueue1, firstJob, 250);

	assertEquals(
		await mq.popAny(),
		firstJob,
		"popAny() must return the first job",
	);

	await new Promise((r) => setTimeout(r, 600));

	// job should be same as the first as timeout expired
	assertEquals(
		await mq.popAny(),
		firstJob,
		"popAny() must return after timeout",
	);

	assertEquals(
		await mq.popAny(),
		secondJob,
		"popAny() must return second job",
	);

	await new Promise((r) => setTimeout(r, 600));

	await mq.complete(myQueue1, firstJob);

	// job should be same as the second
	assertEquals(
		await mq.popAny(),
		secondJob,
		"popAny() must return correct job",
	);

	assertEquals(
		await mq.popAny(),
		undefined,
		"popAny() must return nothing",
	);

	await new Promise((r) => setTimeout(r, 600));

	assertEquals(
		await mq.popAny(),
		secondJob,
		"popAny() must return the retryed job",
	);

	await new Promise((r) => setTimeout(r, 600));

	assertEquals(
		await mq.popAny(),
		secondJob,
		"popAny() must return the retryed job",
	);

	await mq.complete(myQueue2, secondJob);

	assertEquals(
		await mq.popAny(),
		undefined,
		"popAny() must return nothing",
	);
})();

await (async () => {
	console.log("Testing popAny with specified queues...");

	const myQueue1 = "queue1";
	const myQueue2 = "queue2";
	type Queue = typeof myQueue1 | typeof myQueue2;

	type Job = {
		id: string;
		numbers: number[];
	};

	const mq = createMultiQueue<Queue, Job>({
		redis,
		prefix: "multiqueue-popany-specified-queues:",
		retryAfter: 500,
	});

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		undefined,
		"popAny() must return undefined when no jobs",
	);

	const firstJob: Job = {
		id: "unique-id-here",
		numbers: [Math.random()],
	};

	const secondJob: Job = {
		id: "unique-id-here-2",
		numbers: [Math.random()],
	};

	await mq.push(myQueue2, secondJob, 500);
	await mq.push(myQueue1, firstJob, 250);

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		firstJob,
		"popAny() must return the first job",
	);

	await new Promise((r) => setTimeout(r, 600));

	// job should be same as the first as timeout expired
	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		firstJob,
		"popAny() must return after timeout",
	);

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		secondJob,
		"popAny() must return second job",
	);

	await new Promise((r) => setTimeout(r, 600));

	await mq.complete(myQueue1, firstJob);

	// job should be same as the second
	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		secondJob,
		"popAny() must return correct job",
	);

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		undefined,
		"popAny() must return nothing",
	);

	await new Promise((r) => setTimeout(r, 600));

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		secondJob,
		"popAny() must return the retryed job",
	);

	await new Promise((r) => setTimeout(r, 600));

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		secondJob,
		"popAny() must return the retryed job",
	);

	await mq.complete(myQueue2, secondJob);

	assertEquals(
		await mq.popAny([myQueue1, myQueue2]),
		undefined,
		"popAny() must return nothing",
	);
})();

await (async () => {
	console.log("Testing retry functionality...");

	const myQueue = "images-to-be-processed";
	type Queue = typeof myQueue;

	type Job = {
		id: string;
		numbers: number[];
	};

	const mq = createMultiQueue<Queue, Job>({
		redis,
		prefix: "multiqueue-test-2",
		retryAfter: 500,
	});

	assertEquals(
		await mq.pop(myQueue),
		undefined,
		"pop() must return undefined when no jobs",
	);

	const firstJob: Job = {
		id: "unique-id-here",
		numbers: [Math.random()],
	};

	const secondJob: Job = {
		id: "unique-id-here-2",
		numbers: [Math.random()],
	};

	await mq.push(myQueue, secondJob, 500);
	await mq.push(myQueue, firstJob, 250);

	assertEquals(
		await mq.pop(myQueue),
		firstJob,
		"pop() must return correct job",
	);

	await new Promise((r) => setTimeout(r, 600));

	// job should be same as the first as timeout expired
	assertEquals(
		await mq.pop(myQueue),
		firstJob,
		"pop() must return correct job",
	);

	assertEquals(
		await mq.pop(myQueue),
		secondJob,
		"pop() must return correct job",
	);

	await new Promise((r) => setTimeout(r, 600));

	await mq.complete(myQueue, firstJob);

	// job should be same as the second
	assertEquals(
		await mq.pop(myQueue),
		secondJob,
		"pop() must return correct job",
	);

	await mq.complete(myQueue, secondJob);

	assertEquals(
		await mq.pop(myQueue),
		undefined,
		"pop() must return undefined when no jobs",
	);
})();
