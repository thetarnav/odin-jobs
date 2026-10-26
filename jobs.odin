package odin_jobs

import "base:runtime"
import "core:sync"
import "core:os"
import "core:thread"

Job :: struct ($R: typeid) where size_of(R) <= size_of([2]u64) {
	procedure: rawptr,
	data_buff: [2]u64,
	data_size: u8,
}

Job_Internal :: struct {
	generation:  int,
	procedure:   rawptr,
	dispatch:    proc (^Job_Internal),
	data_buff:   [2]u64,
	result_buff: [2]u64,
	data_size:   u8,
	result_size: u8,
	state:       Job_State, // atomic
	to_release:  bool,
}

Job_State :: enum u8 {Init, Ready, Running, Finished, Released}

Jobs_State :: struct {
	ctx:     runtime.Context,
	mutex:   sync.Mutex,
	jobs:    map[Job(rawptr)]Job_Internal,
	threads: []^thread.Thread,
	done:    bool, // atomic
}

init :: proc (state: ^Jobs_State, allocator := context.allocator, loc := #caller_location) {

	state.ctx     = context
	state.jobs    = make(map[Job(rawptr)]Job_Internal, 1000, allocator, loc)
	state.threads = make([]^thread.Thread, os.get_processor_core_count()-1, allocator, loc)

	context.user_ptr = state
	for &t, i in state.threads {
		context.user_index = i
		t = thread.create_and_start(_thread_proc, context)
	}
}
fini :: proc (state: ^Jobs_State) {
	for _, &j in state.jobs {
		_job_execute(state, &j)
	}
	sync.atomic_store(&state.done, true)
	for &t in state.threads {
		thread.destroy(t)
	}
	delete(state.threads)
	delete(state.jobs)
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
	_job_key_release(state, auto_cast job)
}
invalidate :: proc (state: ^Jobs_State, job: Job($R)) {
	_job_key_invalidate(state, auto_cast job)
}

_thread_proc :: proc () {
	state := cast(^Jobs_State)context.user_ptr
	// t := &state.threads[context.user_index]

	for sync.atomic_load(&state.done) == false {
		_do_work(state)
		thread.yield()
	}
}

_do_work :: proc (state: ^Jobs_State) {
	sync.lock(&state.mutex)
	for _, &job in state.jobs {
		if sync.atomic_load(&job.state) == .Ready {
			sync.unlock(&state.mutex)
			_job_execute(state, &job)
			return
		}
	}
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

_job_execute :: proc (state: ^Jobs_State, job: ^Job_Internal) -> bool {
	genration := sync.atomic_load(&job.generation)
	sync.atomic_compare_exchange_strong(&job.state, .Ready, .Running) or_return

	context = state.ctx
	job->dispatch()

	if genration == sync.atomic_load(&job.generation) {
		sync.atomic_store(&job.state, .Finished)
	} else if sync.atomic_load(&job.to_release) {
		sync.atomic_store(&job.state, .Released)
		_job_remove(state, _job_key(job^))
	}
	return true
}

_job_key_release :: proc (state: ^Jobs_State, id: Job(rawptr)) {
	if job, in_cache := _job_get(state, id); in_cache {
		_job_release(state, job)
	}
}
_job_release :: proc (state: ^Jobs_State, job: ^Job_Internal) {
	sync.atomic_store(&job.to_release, true)
	if _, finished := sync.atomic_compare_exchange_strong(&job.state, .Finished, .Released); finished {
		_job_remove(state, _job_key(job^))
	}
}

_job_key_invalidate :: proc (state: ^Jobs_State, id: Job(rawptr)) {
	if job, in_cache := _job_get(state, id); in_cache {
		_job_invalidate(state, job)
	}
}
_job_invalidate :: proc (state: ^Jobs_State, job: ^Job_Internal) {
	sync.atomic_add(&job.generation, 1)
	sync.atomic_store(&job.to_release, true)
	if _, finished := sync.atomic_compare_exchange_strong(&job.state, .Finished, .Released); finished {
		_job_remove(state, _job_key(job^))
	} else if _, ready := sync.atomic_compare_exchange_strong(&job.state, .Ready, .Released); ready {
		_job_remove(state, _job_key(job^))
	}
}

_job_get :: proc (state: ^Jobs_State, id: Job(rawptr)) -> (^Job_Internal, bool) {
	sync.guard(&state.mutex)
	return &state.jobs[id]
}
_job_add :: proc (state: ^Jobs_State, job: Job_Internal) -> Job(rawptr) {
	sync.guard(&state.mutex)
	state.jobs[_job_key(job)] = job
	return _job_key(job)
}
_job_remove :: proc (state: ^Jobs_State, id: Job(rawptr)) {
	sync.guard(&state.mutex)
	delete_key(&state.jobs, id)
}

_job_key :: proc (job: Job_Internal) -> Job(rawptr) {
	return Job(rawptr){job.procedure, job.data_buff, job.data_size}
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

	if job, cached := _job_get(state, {procedure, data_buff, data_size}); cached {
		assert(job.result_size == result_size)
		sync.atomic_store(&job.to_release, false)
		return {procedure, data_buff, data_size}
	}

	return _job_add(state, {
		procedure   = procedure,
		dispatch    = dispatch,
		data_buff   = data_buff,
		data_size   = data_size,
		result_size = result_size,
		state       = .Ready,
	})
}

_wait_raw :: proc (
	state: ^Jobs_State, id: Job(rawptr), res: rawptr,
) -> (found: bool) {
	job := _job_get(state, id) or_return

	if sync.atomic_load(&job.state) < .Finished {
		if !_job_execute(state, job) {
			for sync.atomic_load(&job.state) < .Finished {
				_do_work(state)
			}
		}
	}

	if job.result_size > 0 {
		runtime.mem_copy_non_overlapping(res, &job.result_buff, int(job.result_size))
	}
	return true
}

