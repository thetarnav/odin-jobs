package odin_jobs_tests

import "core:sync"
import "core:thread"
import "core:testing"
import jobs ".."

@test
test_basic :: proc (t: ^testing.T) {

	jobs_state: jobs.Jobs_State
	jobs.init(state=&jobs_state)
	defer jobs.fini(&jobs_state)

	{// Simple return value
		job := jobs.job(proc () -> int {
			thread.yield()
			return 1
		}, &jobs_state)
		defer jobs.release(job, &jobs_state)

		res := jobs.wait(job, &jobs_state)
		testing.expect_value(t, res, 1)
	}

	{// Simple take data and return value
		job := jobs.job(proc (a: int) -> int {
			thread.yield()
			return a + 1
		}, 4, &jobs_state)
		defer jobs.release(job, &jobs_state)

		res := jobs.wait(job, &jobs_state)
		testing.expect_value(t, res, 5)
	}

	{// Deduplication
		count: int
		js: [dynamic]jobs.Job(struct {})
		js.allocator = context.temp_allocator

		for _ in 0..<5 {
			job := jobs.job(proc (a: ^int) {
				thread.yield()
				sync.atomic_add(a, 1)
			}, &count, &jobs_state)
			append(&js, job)
		}

		for j in js {
			jobs.wait(j, &jobs_state)
		}
		for j in js {
			jobs.release(j, &jobs_state)
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
		job1 := jobs.job(procedure, {&count, &val}, &jobs_state)

		jobs.invalidate(job1, &jobs_state)
		jobs.wait(job1, &jobs_state)

		val = 2
		job2 := jobs.job(procedure, {&count, &val}, &jobs_state)
		testing.expect(t, job1 != job2, "slot reuse did not advance the job generation")
		_, stale_found := jobs.wait(job1, &jobs_state)
		testing.expect(t, !stale_found, "old job handle resolved to a reused slot")

		jobs.wait(job2, &jobs_state)
		jobs.release(job2, &jobs_state)

		testing.expect_value(t, count, 2)
	}
}

@test
test_invalidate_running_job_does_not_return_outdated_result :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(state=&jobs_state)
	_stop_job_workers(&jobs_state)
	defer jobs.fini(&jobs_state)

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
		wait.result, wait.found = jobs.wait(wait.job, wait.state)
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

	started, proceed: bool
	job := jobs.job(proc (gate: Invalidate_Running_Gate) -> int {
		sync.atomic_store(gate.started, true)
		for !sync.atomic_load(gate.proceed) {
			thread.yield()
		}
		return 99
	}, Invalidate_Running_Gate{&started, &proceed}, &jobs_state)

	wait: Invalidate_Running_Wait
	wait.state = &jobs_state
	wait.job = job
	waiter := thread.create_and_start_with_data(rawptr(&wait), _invalidate_running_wait_proc)
	assert(waiter != nil)

	for !sync.atomic_load(&started) {
		thread.yield()
	}

	jobs.invalidate(job, &jobs_state)
	sync.atomic_store(&proceed, true)
	thread.destroy(waiter)

	testing.expect(t, !wait.found, "invalidated running job returned a result")
}

@test
test_job_can_submit_and_wait_for_child :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(state=&jobs_state)
	defer jobs.fini(&jobs_state)

	parent := jobs.job(proc (state: ^jobs.Jobs_State) -> int {
		child := jobs.job(proc (value: int) -> int {
			return value * 2
		}, 21, state)
		result, found := jobs.wait(child, state)
		jobs.release(child, state)
		if !found {
			return -1
		}
		return result
	}, &jobs_state, &jobs_state)

	result, found := jobs.wait(parent, &jobs_state)
	jobs.release(parent, &jobs_state)
	testing.expect(t, found, "parent job did not complete")
	testing.expect_value(t, result, 42)
}

@test
test_fini_drains_unwaited_jobs :: proc (t: ^testing.T) {
	jobs_state: jobs.Jobs_State
	jobs.init(state=&jobs_state)

	count: int
	_ = jobs.job(proc (value: ^int) {
		sync.atomic_add(value, 1)
	}, &count, &jobs_state)

	jobs.fini(&jobs_state)
	testing.expect_value(t, sync.atomic_load(&count), 1)
}
