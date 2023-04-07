import type { CreateMultiQueue, CreateMultiQueueOptions } from "./types.ts";

export const createMultiQueue: CreateMultiQueue = <Queue, Job>(
	{ redis, prefix, retryAfter }: CreateMultiQueueOptions,
) => {
	const queueDepthKey = `${prefix}depth`;
	const tempKey = `${prefix}temp`;

	const queueKeyPrefix = `${prefix}queue:`;
	const retryKeyPrefix = `${prefix}retry:`;

	const queueKey = (queue: Queue) =>
		`${queueKeyPrefix}${JSON.stringify(queue)}`;

	const retryKey = (queue: Queue) =>
		`${retryKeyPrefix}${JSON.stringify(queue)}`;

	const push = async (queue: Queue, job: Job, priority: number) => {
		await redis.eval(
			`
				-- increment queue depth
				redis.call('zincrby', KEYS[1], 1, ARGV[1])
				
				-- push to queue
				redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
            `,
			[
				queueDepthKey,
				queueKey(queue),
			],
			[
				JSON.stringify(queue),
				priority.toString(),
				JSON.stringify(job),
			],
		);
	};

	const pop = async (queue: Queue) => {
		const job = await redis.eval(
			`
				-- pop oldest in-progress job
				local retryJob = redis.call('zpopmin', KEYS[1])

				-- if retry job exists
				if tonumber(retryJob[2]) ~= nil then
					-- if older than timeout, get this job
					if tonumber(retryJob[2]) < tonumber(ARGV[1]) then
						-- update job time
						redis.call('zadd', KEYS[1], ARGV[2], retryJob[1])

						return retryJob[1]
					end

					-- job not beyond timeout, add back to queue
					redis.call('zadd', KEYS[1], retryJob[2], retryJob[1])
				end

				-- get new job
				local newJob = redis.call('zpopmin', KEYS[2])

				if tonumber(newJob[2]) ~= nil then
					-- add to retry queue
					redis.call('zadd', KEYS[1], ARGV[2], newJob[1])
					-- decrement queue depth
					redis.call('zincrby', KEYS[3], -1, ARGV[3])
				end

				return newJob[1]
            `,
			[
				retryKey(queue),
				queueKey(queue),
				queueDepthKey,
			],
			[
				Date.now() - retryAfter,
				Date.now(),
				JSON.stringify(queue),
			],
		);

		return typeof job === "string" ? JSON.parse(job) as Job : undefined;
	};

	const complete = async (queue: Queue, job: Job) => {
		await redis.eval(
			`
				-- remove from retry queue
				redis.call('zrem', KEYS[1], ARGV[1])
            `,
			[
				retryKey(queue),
			],
			[
				JSON.stringify(job),
			],
		);
	};

	const getDeepest = async () => {
		const queue = await redis.eval(
			`
				return redis.call('zrange', KEYS[1], 0, 0, 'BYSCORE', 'REV')
            `,
			[
				queueDepthKey,
			],
			[],
		);

		return typeof queue === "string"
			? JSON.parse(queue) as Queue
			: undefined;
	};

	const popAny = async () => {
		const job = await redis.eval(
			`
				-- global variables
				local queueDepthKey = KEYS[1]
				local tempKey = KEYS[2]
				local queueKeyPrefix = ARGV[1]
				local retryKeyPrefix = ARGV[2]
				local now = ARGV[3]
				local timeout = ARGV[4]

				-- get list of queues
				local queues = redis.call('zrange', KEYS[1], '0', '+inf', 'BYSCORE')

				local oldestTime = tonumber(now)
				local oldestJob = nil
				local oldestQueue = nil

				for k,queue in pairs(queues) do
					local jobResponse = redis.call('zrange', retryKeyPrefix .. queue, '0', '0', 'WITHSCORES')

					if jobResponse[1] ~= nil then
						if tonumber(jobResponse[2]) < oldestTime then
							oldestTime = tonumber(jobResponse[2])
							oldestJob = jobResponse[1]
							oldestQueue = queue
						end
					end
				end

				if oldestTime < tonumber(timeout) then
					redis.call('zadd', retryKeyPrefix .. oldestQueue, now, oldestJob)

					return oldestJob
				end

				oldestTime = tonumber(now)
				oldestJob = nil
				oldestQueue = nil

				for k,queue in pairs(queues) do
					local jobResponse = redis.call('zrange', queueKeyPrefix .. queue, '0', '0', 'WITHSCORES')

					if jobResponse[1] ~= nil then
						if tonumber(jobResponse[2]) < oldestTime then
							oldestTime = tonumber(jobResponse[2])
							oldestJob = jobResponse[1]
							oldestQueue = queue
						end
					end
				end

				if oldestJob ~= nil then
					redis.call('zrem', queueKeyPrefix .. oldestQueue, oldestJob)
					redis.call('zadd', retryKeyPrefix .. oldestQueue, now, oldestJob)
					return oldestJob
				end
            `,
			[
				queueDepthKey,
				tempKey,
			],
			[
				queueKeyPrefix,
				retryKeyPrefix,
				Date.now(),
				Date.now() - retryAfter,
			],
		);

		return typeof job === "string" ? JSON.parse(job) as Job : undefined;
	};

	return {
		push,
		pop,
		complete,
		getDeepest,
		popAny,
	};
};
