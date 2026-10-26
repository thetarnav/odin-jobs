package odin_jobs

import "base:runtime"
import "core:sync"
import "core:os"
import "core:thread"
import hm "core:container/handle_map"
import "core:container/queue"

Job :: struct ($R: typeid) where size_of(R) <= size_of([2]u64) {
	idx, gen: u32,
}

Job_Internal :: struct {
	using handle: Job(rawptr),
	procedure:    rawptr,
	dispatch:     proc (^Job_Internal),
	data_buff:    [2]u64,
	result_buff:  [2]u64,
	data_size:    u8,
	result_size:  u8,
	state:        Job_State,
	cached:       bool,
	to_release:   bool,
	invalidated:  bool,
	waiters:      int,
}

Job_State :: enum u8 {Init, Ready, Running, Finished, Released}

Jobs_State :: struct {
	ctx:             runtime.Context,
	mutex:           sync.Mutex,
	jobs:            hm.Dynamic_Handle_Map(Job_Internal, Job(rawptr)),
	ready:           queue.Queue(Job(rawptr)),
	work_available:  sync.Sema,
	job_changed:     sync.Cond,
	threads:         []^thread.Thread,
	outstanding:     int, // Ready + Running; protected by mutex
	done:            bool, // atomic
}

init :: proc (state: ^Jobs_State, allocator := context.allocator, loc := #caller_location) {
	state.ctx         = context
	state.done        = false
	state.outstanding = 0

	hm.dynamic_init(&state.jobs, allocator)
	if err := queue.init(&state.ready, 64, allocator, loc); err != nil {
		panic("odin_jobs: failed to initialize ready queue")
	}

	worker_count := max(os.get_processor_core_count()-1, 0)
	state.threads = make([]^thread.Thread, worker_count, allocator, loc)

	context.user_ptr = state
	for &t, i in state.threads {
		context.user_index = i
		t = thread.create_and_start(_thread_proc, context)
		assert(t != nil, "odin_jobs: failed to start worker thread")
	}
}

fini :: proc (state: ^Jobs_State) {
	// Help drain ready work. If all outstanding jobs are running elsewhere,
	// sleep until one completes or enqueues more work.
	for {
		sync.lock(&state.mutex)
		handle, _, got_job := _claim_next_locked(state)
		if got_job {
			sync.unlock(&state.mutex)
			_job_run(state, handle)
			continue
		}

		if state.outstanding == 0 {
			sync.atomic_store(&state.done, true)
			sync.unlock(&state.mutex)
			break
		}

		sync.cond_wait(&state.job_changed, &state.mutex)
		sync.unlock(&state.mutex)
	}

	// Wake idle workers so they can observe done and exit. Never join while
	// holding the scheduler mutex.
	if len(state.threads) > 0 {
		sync.sema_post(&state.work_available, len(state.threads))
	}
	for &t in state.threads {
		thread.destroy(t)
	}

	delete(state.threads)
	state.threads = nil
	queue.destroy(&state.ready)
	hm.dynamic_destroy(&state.jobs)
}

job_data_result :: proc (
	state: ^Jobs_State, procedure: proc ($D) -> $R, data: D,
) -> (job: Job(R)) where
	size_of(D) <= size_of([2]u64),
	size_of(R) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), size_of(R), _job_dispatch_proc(D, R))
}
job_no_data_result :: proc (
	state: ^Jobs_State, procedure: proc () -> $R,
) -> (job: Job(R)) where
	size_of(R) <= size_of([2]u64) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, size_of(R), _job_dispatch_proc(struct {}, R))
}
job_data_no_result :: proc (
	state: ^Jobs_State, procedure: proc ($D), data: D,
) -> (job: Job(struct {})) where
	size_of(D) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), 0, _job_dispatch_proc(D, struct {}))
}
job_no_data_no_result :: proc (
	state: ^Jobs_State, procedure: proc (),
) -> (job: Job(struct {})) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, 0, _job_dispatch_proc(struct {}, struct {}))
}

job :: proc {job_data_result, job_no_data_result, job_data_no_result, job_no_data_no_result}

wait :: proc (state: ^Jobs_State, id: Job($R)) -> (res: R, found: bool) #optional_ok {
	found = _wait_raw(state, auto_cast id, &res)
	return
}

release :: proc (state: ^Jobs_State, job: Job($R)) {
	_job_release_handle(state, auto_cast job)
}
invalidate :: proc (state: ^Jobs_State, job: Job($R)) {
	_job_invalidate_handle(state, auto_cast job)
}

_thread_proc :: proc () {
	state := cast(^Jobs_State)context.user_ptr
	for {
		sync.sema_wait(&state.work_available)
		if _do_work(state) {
			continue
		}
		if sync.atomic_load(&state.done) {
			return
		}
	}
}

_do_work :: proc (state: ^Jobs_State) -> bool {
	sync.lock(&state.mutex)
	handle, _, got_job := _claim_next_locked(state)
	sync.unlock(&state.mutex)
	if got_job {
		_job_run(state, handle)
	}
	return got_job
}

_claim_next_locked :: proc (state: ^Jobs_State) -> (handle: Job(rawptr), job: ^Job_Internal, got_job: bool) {
	for queue.len(state.ready) > 0 {
		handle = queue.pop_front(&state.ready)
		job = hm.dynamic_get(&state.jobs, handle) or_continue
		if job.state != .Ready || job.invalidated {
			continue
		}
		job.state = .Running
		got_job = true
		return
	}
	return
}

_job_run :: proc (state: ^Jobs_State, handle: Job(rawptr)) {
	// A running slot is not reclaimed or reused until this procedure publishes
	// completion, so the handle continues to resolve throughout dispatch.
	sync.lock(&state.mutex)
	job, valid := hm.dynamic_get(&state.jobs, handle)
	assert(valid && job.state == .Running)
	sync.unlock(&state.mutex)

	context = state.ctx
	job->dispatch()

	sync.lock(&state.mutex)
	job, valid = hm.dynamic_get(&state.jobs, handle)
	assert(valid && job.state == .Running)

	state.outstanding -= 1
	if job.invalidated {
		job.state = .Released
		job.cached = false
	} else {
		job.state = .Finished
		if job.to_release {
			job.cached = false
		}
	}

	_maybe_reclaim_locked(state, handle, job)
	sync.cond_broadcast(&state.job_changed)
	sync.unlock(&state.mutex)
}

_job_dispatch_proc :: proc ($D, $R: typeid) -> proc (^Job_Internal) {
	return proc (job: ^Job_Internal) {
		when size_of(D) == 0 && size_of(R) == 0 {
			(proc ())(job.procedure)()
		}
		else when size_of(D) == 0 {
			res := (proc () -> R)(job.procedure)()
			runtime.mem_copy_non_overlapping(&job.result_buff, &res, size_of(R))
		}
		else when size_of(R) == 0 {
			data: D
			runtime.mem_copy_non_overlapping(&data, &job.data_buff, size_of(D))
			(proc (D))(job.procedure)(data)
		}
		else {
			data: D
			runtime.mem_copy_non_overlapping(&data, &job.data_buff, size_of(D))
			res := (proc (D) -> R)(job.procedure)(data)
			runtime.mem_copy_non_overlapping(&job.result_buff, &res, size_of(R))
		}
	}
}

_job_release_handle :: proc (state: ^Jobs_State, handle: Job(rawptr)) {
	sync.guard(&state.mutex)
	if job, valid := hm.dynamic_get(&state.jobs, handle);
	   valid && job.cached && !job.invalidated {
		job.to_release = true
		if job.state == .Finished {
			job.cached = false
			_maybe_reclaim_locked(state, handle, job)
		}
	}
	sync.cond_broadcast(&state.job_changed)
}

_job_invalidate_handle :: proc (state: ^Jobs_State, handle: Job(rawptr)) {
	sync.guard(&state.mutex)
	if job, valid := hm.dynamic_get(&state.jobs, handle);
	   valid && job.cached && !job.invalidated {
		job.invalidated = true
		job.cached = false
		job.to_release = true
		if job.state == .Ready {
			job.state = .Released
			state.outstanding -= 1
		} else if job.state == .Finished {
			job.state = .Released
		}
		_maybe_reclaim_locked(state, handle, job)
	}
	sync.cond_broadcast(&state.job_changed)
}

_find_key_locked :: proc (
	state: ^Jobs_State,
	procedure: rawptr,
	data_buff: [2]u64,
	data_size: u8,
) -> (handle: Job(rawptr), job: ^Job_Internal, found: bool) {
	it := hm.dynamic_iterator_make(&state.jobs)
	for {
		slot, h := hm.dynamic_iterate(&it) or_break
		if slot.cached && !slot.invalidated &&
		   slot.procedure == procedure &&
		   slot.data_buff == data_buff &&
		   slot.data_size == data_size {
			return h, slot, true
		}
	}
	return
}

_maybe_reclaim_locked :: proc (state: ^Jobs_State, handle: Job(rawptr), job: ^Job_Internal) {
	if job.cached || job.waiters != 0 || job.state == .Running || job.handle.idx == 0 {
		return
	}
	job.state = .Released
	removed, err := hm.dynamic_remove(&state.jobs, handle)
	assert(err == nil, "odin_jobs: failed to release job slot")
	assert(removed, "odin_jobs: job slot was already released")
}

_job_raw :: proc (
	state: ^Jobs_State,
	procedure, data: rawptr,
	data_size, result_size: u8,
	dispatch: proc (^Job_Internal),
) -> Job(rawptr) {

	data_buff: [2]u64
	if data_size > 0 {
		runtime.mem_copy_non_overlapping(&data_buff, data, int(data_size))
	}

	sync.guard(&state.mutex)

	assert(!sync.atomic_load(&state.done), "odin_jobs: submitting a job after fini")
	if cached_handle, cached_job, cached := _find_key_locked(state, procedure, data_buff, data_size); cached {
		cached_job.to_release = false
		return cached_handle
	}

	entry := Job_Internal{
		procedure   = procedure,
		dispatch    = dispatch,
		data_buff   = data_buff,
		data_size   = data_size,
		result_size = result_size,
		state       = .Ready,
		cached      = true,
	}
	handle, err := hm.dynamic_add(&state.jobs, entry)
	assert(err == nil, "odin_jobs: failed to allocate job slot")

	slot, valid := hm.dynamic_get(&state.jobs, handle)
	assert(valid)

	slot.handle = handle
	ok, queue_err := queue.push_back(&state.ready, handle)
	assert(ok && queue_err == nil, "odin_jobs: failed to enqueue job")

	state.outstanding += 1

	sync.sema_post(&state.work_available)
	return handle
}

_wait_raw :: proc (
	state: ^Jobs_State, handle: Job(rawptr), res: rawptr,
) -> (found: bool) {

	sync.guard(&state.mutex)

	initial_job := hm.dynamic_get(&state.jobs, handle) or_return
	if !initial_job.cached || initial_job.invalidated {
		return false
	}
	initial_job.waiters += 1

	for {
		job, valid := hm.dynamic_get(&state.jobs, handle)
		if !valid || job.invalidated || job.state == .Released {
			found = false
			break
		}
		if job.state == .Finished {
			if job.result_size > 0 {
				runtime.mem_copy_non_overlapping(res, &job.result_buff, int(job.result_size))
			}
			found = true
			break
		}
		if job.state == .Ready {
			job.state = .Running
			sync.unlock(&state.mutex)
			_job_run(state, handle)
			sync.lock(&state.mutex)
			continue
		}

		// Help with other queued jobs before sleeping. This avoids deadlock when
		// all workers are waiting on child jobs submitted by running jobs.
		if helper_handle, _, got_job := _claim_next_locked(state); got_job {
			sync.unlock(&state.mutex)
			_job_run(state, helper_handle)
			sync.lock(&state.mutex)
			continue
		}
		sync.cond_wait(&state.job_changed, &state.mutex)
	}

	if final_job, valid := hm.dynamic_get(&state.jobs, handle); valid {
		final_job.waiters -= 1
		_maybe_reclaim_locked(state, handle, final_job)
	}

	return
}
