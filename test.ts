import { assertEquals } from "https://deno.land/std@0.181.0/testing/asserts.ts";
import { connect } from "./deps.ts";
import { createMultiQueue } from "./main.ts";

const redis = await connect({
	hostname: "127.0.0.1",
});

await redis.flushall();

(async () => {
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

(async () => {
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
