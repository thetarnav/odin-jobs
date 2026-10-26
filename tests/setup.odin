package odin_jobs_tests

import "core:sync"
import "core:thread"
import "core:testing"
import jobs ".."

Invalidate_Running_Gate :: struct {
	started: ^bool,
	proceed: ^bool,
}

Invalidate_Running_Wait :: struct {
	state:  ^jobs.Jobs_State,
	job:    jobs.Job(int),
	found:  bool,
	result: int,
}

_invalidate_running_wait_proc :: proc (data: rawptr) {
	wait := cast(^Invalidate_Running_Wait)data
	wait.result, wait.found = jobs.wait(wait.state, wait.job)
}

_stop_job_workers :: proc (state: ^jobs.Jobs_State) {
	sync.atomic_store(&state.done, true)
	if len(state.threads) > 0 {
		sync.sema_post(&state.work_available, len(state.threads))
	}
	for t in state.threads {
		thread.destroy(t)
	}
	delete(state.threads)
	state.threads = nil
	sync.atomic_store(&state.done, false)
}

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
		testing.expect(t, job1 != job2, "slot reuse did not advance the job generation")
		_, stale_found := jobs.wait(&jobs_state, job1)
		testing.expect(t, !stale_found, "old job handle resolved to a reused slot")

		jobs.wait(&jobs_state, job2)
		jobs.release(&jobs_state, job2)

		testing.expect_value(t, count, 2)
	}
}

@test
test_invalidate_running_job_does_not_return_outdated_result :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(&jobs_state)
	_stop_job_workers(&jobs_state)
	defer jobs.fini(&jobs_state)

	started, proceed: bool
	job := jobs.job(&jobs_state, proc (gate: Invalidate_Running_Gate) -> int {
		sync.atomic_store(gate.started, true)
		for !sync.atomic_load(gate.proceed) {
			thread.yield()
		}
		return 99
	}, Invalidate_Running_Gate{&started, &proceed})

	wait: Invalidate_Running_Wait
	wait.state = &jobs_state
	wait.job = job
	waiter := thread.create_and_start_with_data(rawptr(&wait), _invalidate_running_wait_proc)
	assert(waiter != nil)

	for !sync.atomic_load(&started) {
		thread.yield()
	}

	jobs.invalidate(&jobs_state, job)
	sync.atomic_store(&proceed, true)
	thread.destroy(waiter)

	testing.expect(t, !wait.found, "invalidated running job returned a result")
}

@test
test_job_can_submit_and_wait_for_child :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(&jobs_state)
	defer jobs.fini(&jobs_state)

	parent := jobs.job(&jobs_state, proc (state: ^jobs.Jobs_State) -> int {
		child := jobs.job(state, proc (value: int) -> int {
			return value * 2
		}, 21)
		result, found := jobs.wait(state, child)
		jobs.release(state, child)
		if !found {
			return -1
		}
		return result
	}, &jobs_state)

	result, found := jobs.wait(&jobs_state, parent)
	jobs.release(&jobs_state, parent)
	testing.expect(t, found, "parent job did not complete")
	testing.expect_value(t, result, 42)
}

@test
test_fini_drains_unwaited_jobs :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(&jobs_state)

	count: int
	_ = jobs.job(&jobs_state, proc (value: ^int) {
		sync.atomic_add(value, 1)
	}, &count)

	jobs.fini(&jobs_state)
	testing.expect_value(t, sync.atomic_load(&count), 1)
}
