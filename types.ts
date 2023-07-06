import type { Redis } from "./deps.ts";

// https://github.com/sindresorhus/type-fest
type JsonObject =
	& { [Key in string]: JsonValue }
	& { [Key in string]?: JsonValue | undefined };
type JsonArray = JsonValue[] | readonly JsonValue[];
type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonObject | JsonArray;

export type MultiQueue<Queue extends JsonValue, Job extends JsonValue> = {
	push: (queue: Queue, job: Job, priority: number) => Promise<void>;
	pop: (queue: Queue) => Promise<Job | undefined>;
	complete: (queue: Queue, job: Job) => Promise<void>;
	getDeepest: () => Promise<Queue | undefined>;
	popAny: (queues?: Queue[]) => Promise<Job | undefined>;
};

export type CreateMultiQueueOptions = {
	redis: Redis;
	prefix: string;
	retryAfter: number;
};

export type CreateMultiQueue = <Queue extends JsonValue, Job extends JsonValue>(
	options: CreateMultiQueueOptions,
) => MultiQueue<Queue, Job>;
