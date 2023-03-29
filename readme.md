# multiqueue

Atomic first-in-first-out Redis queue for Deno.

## features

- Zero-dependencies outside core Redis types
- First-class TypeScript support with generics
- Auto-retry functionality with timeout
- Automatic test suite

## usage

The following code imagines you have two queue pipelines: one for resizing images and one for resizing videos.

```typescript
type Queue = "resize-image" | "resize-video";

type Job = {
	id: number;
	path: string;
	resizeWidth: number;
};

const mq = createMultiQueue<Queue, Job>({
	redis,
	prefix: "multiqueue",
	retryAfter: 60 * 1000, // retry if not completed within 1 minute
});

await mq.push("resize-image", {
	id: 1,
	path: "my-image.jpg",
	resizeWidth: 1024,
});

const imageToResize: Job = await mq.pop("resize-image");

// do resizing work

await mq.complete(imageToResize);
```
