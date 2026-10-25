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

	{
		job := jobs.job(&jobs_state, proc () -> int {
			thread.yield()
			return 1
		})

		res := jobs.wait(&jobs_state, job)
		testing.expect_value(t, res, 1)
	}

	{
		job := jobs.job(&jobs_state, proc (a: int) -> int {
			thread.yield()
			return a + 1
		}, 4)

		res := jobs.wait(&jobs_state, job)
		testing.expect_value(t, res, 5)
	}

	{
		count: int
		js: [dynamic]jobs.Job(^int, struct {})
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
		testing.expect_value(t, sync.atomic_load(&count), 1)
	}
}
