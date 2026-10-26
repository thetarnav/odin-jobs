package odin_jobs_tests

import "core:sync"
import "core:thread"
import "core:testing"
import jobs ".."

@test
test_basic :: proc (t: ^testing.T) {

	jobs_state: jobs.Jobs_State
	jobs.init(&jobs_state)
	defer jobs.fini(&jobs_state)

	{// Simple return value
		job := jobs.job(&jobs_state, proc () -> int {
			thread.yield()
			return 1
		})
		defer jobs.release(&jobs_state, job)

		res := jobs.wait(&jobs_state, job)
		testing.expect_value(t, res, 1)
	}

	{// Simple take data and return value
		job := jobs.job(&jobs_state, proc (a: int) -> int {
			thread.yield()
			return a + 1
		}, 4)
		defer jobs.release(&jobs_state, job)

		res := jobs.wait(&jobs_state, job)
		testing.expect_value(t, res, 5)
	}

	{// Deduplication
		count: int
		js: [dynamic]jobs.Job(struct {})
		js.allocator = context.temp_allocator

		for _ in 0..<5 {
			job := jobs.job(&jobs_state, proc (a: ^int) {
				thread.yield()
				sync.atomic_add(a, 1)
			}, &count)
			append(&js, job)
		}

		for j in js {
			jobs.wait(&jobs_state, j)
		}
		for j in js {
			jobs.release(&jobs_state, j)
		}
		testing.expect_value(t, sync.atomic_load(&count), 1)
	}

	{// Invalidate cache
		count: int

		procedure := proc (a: struct {count: ^int, n: ^int}) {
			thread.yield()
			sync.atomic_store(a.count, a.n^)
		}

		val := 1
		job1 := jobs.job(&jobs_state, procedure, {&count, &val})

		jobs.invalidate(&jobs_state, job1)
		jobs.wait(&jobs_state, job1)

		val = 2
		job2 := jobs.job(&jobs_state, procedure, {&count, &val})
		jobs.wait(&jobs_state, job2)
		jobs.release(&jobs_state, job2)

		testing.expect_value(t, count, 2)
	}
}
