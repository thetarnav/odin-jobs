package odin_jobs_tests

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
}
