package odin_jobs

import "core:sync"
import "core:os"
import "core:thread"
import "base:runtime"

// - Jobs take data and procedure
// - data and procedure are hashed
// - created jobs are cached with that hash
// - jobs can be manually invalidated to remove them from the cache
//   or it could be a lru cache
// - data must be allocated and copied if larger than a pointer
//   and it can be freed when the job finishes
// - result if larger than a pointer must be allocated as well
//   it must be alive as long as the job is in the cache

Job :: struct ($D, $R: typeid)
	where size_of(D) <= size_of([2]u64),
	      size_of(R) <= size_of([2]u64) {
	procedure: rawptr,
	data_buff: [2]u64,
	data_size: u8,
}

Job_Internal :: struct {
	procedure:   rawptr,
	dispatch:    proc (^Job_Internal),
	data_buff:   [2]u64,
	result_buff: [2]u64,
	data_size:   u8,
	result_size: u8,
	state:       Job_State, // atomic
}

Job_State :: enum u8 {Init, Ready, Running, Finished}

Jobs_State :: struct {
	ctx:     runtime.Context,
	jobs:    map[Job(rawptr, rawptr)]Job_Internal,
	threads: []^thread.Thread,
	done:    bool, // atomic
}

init :: proc (state: ^Jobs_State, allocator := context.allocator, loc := #caller_location) {

	state.ctx     = context
	state.jobs    = make(map[Job(rawptr, rawptr)]Job_Internal, 1000, allocator, loc)
	state.threads = make([]^thread.Thread, os.get_processor_core_count()-1, allocator, loc)

	context.user_ptr = state
	for &t, i in state.threads {
		context.user_index = i
		t = thread.create_and_start(_thread_proc, context)
	}
}
fini :: proc (state: ^Jobs_State) {
	for _, &j in state.jobs {
		_job_execute(&j)
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
) -> (job: Job(D, R)) where
	size_of(D) <= size_of([2]u64),
	size_of(R) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), size_of(R), _job_dispatch_proc(D, R))
}
job_no_data_result :: proc (
	state: ^Jobs_State, procedure: proc () -> $R,
) -> (job: Job(struct {}, R)) where
	size_of(R) <= size_of([2]u64) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, size_of(R), _job_dispatch_proc(struct {}, R))
}
job_data_no_result :: proc (
	state: ^Jobs_State, procedure: proc ($D), data: D,
) -> (job: Job(D, struct {})) where
	size_of(D) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), 0, _job_dispatch_proc(D, struct {}))
}
job_no_data_no_result :: proc (
	state: ^Jobs_State, procedure: proc (),
) -> (job: Job(struct {}, struct {})) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, 0, _job_dispatch_proc(struct {}, struct {}))
}

job :: proc {job_data_result, job_no_data_result, job_data_no_result, job_no_data_no_result}

wait :: proc (state: ^Jobs_State, id: Job($D, $R)) -> (res: R, found: bool) #optional_ok {
	found = _wait_raw(state, auto_cast id, &res)
	return
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
	for _, &j in state.jobs {
		_job_execute(&j) or_continue
		break
	}
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

_job_key :: proc (job: Job_Internal) -> Job(rawptr, rawptr) {
	return Job(rawptr, rawptr){job.procedure, job.data_buff, job.data_size}
}

_job_raw :: proc (
	state: ^Jobs_State,
	procedure, data: rawptr,
	data_size, result_size: u8,
	dispatch: proc (^Job_Internal),
) -> Job(rawptr, rawptr) {

	data_buff: [2]u64
	if data_size > 0 {
		runtime.mem_copy_non_overlapping(&data_buff, data, int(data_size))
	}

	for id, job in state.jobs {
		if id != {procedure, data_buff, data_size} do continue
		assert(job.result_size == result_size)
		return id
	}

	context = state.ctx

	job := Job_Internal{
		procedure   = procedure,
		dispatch    = dispatch,
		data_buff   = data_buff,
		data_size   = data_size,
		result_size = result_size,
		state       = .Ready,
	}
	state.jobs[_job_key(job)] = job

	return _job_key(job)
}

_job_execute :: proc (job: ^Job_Internal) -> bool {
	if sync.atomic_load(&job.state) == .Ready {
		sync.atomic_store(&job.state, .Running)
		job->dispatch()
		sync.atomic_store(&job.state, .Finished)
		return true
	}
	return false
}

_wait_raw :: proc (
	state: ^Jobs_State, id: Job(rawptr, rawptr), res: rawptr,
) -> (found: bool) {
	for job_id, &job in state.jobs {
		if job_id != id do continue

		if sync.atomic_load(&job.state) != .Finished {
			if !_job_execute(&job) {
				for sync.atomic_load(&job.state) != .Finished {
					_do_work(state)
				}
			}
		}

		if job.result_size > 0 {
			runtime.mem_copy_non_overlapping(res, &job.result_buff, int(job.result_size))
		}
		return true
	}

	// Not found
	return false
}

