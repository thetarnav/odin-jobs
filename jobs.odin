package jobs

_global_state: Jobs_State
_global := &_global_state

init :: proc (allocator := context.allocator, state := _global, loc := #caller_location) {
	_init(state, allocator, loc)
}
fini :: proc (state := _global) {
	_fini(state)
}

job_data_result :: proc (
	procedure: proc ($D) -> $R, data: D,
	state := _global,
) -> (job: Job(R)) where
	size_of(D) <= size_of([2]u64),
	size_of(R) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), size_of(R), _job_dispatch_proc(D, R))
}
job_no_data_result :: proc (
	procedure: proc () -> $R,
	state := _global,
) -> (job: Job(R)) where
	size_of(R) <= size_of([2]u64) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, size_of(R), _job_dispatch_proc(struct {}, R))
}
job_data_no_result :: proc (
	procedure: proc ($D), data: D,
	state := _global,
) -> (job: Job(struct {})) where
	size_of(D) <= size_of([2]u64) {
	data := data
	return auto_cast _job_raw(state, auto_cast procedure, &data, size_of(D), 0, _job_dispatch_proc(D, struct {}))
}
job_no_data_no_result :: proc (
	procedure: proc (),
	state := _global,
) -> (job: Job(struct {})) {
	return auto_cast _job_raw(state, auto_cast procedure, nil, 0, 0, _job_dispatch_proc(struct {}, struct {}))
}

job :: proc {job_data_result, job_no_data_result, job_data_no_result, job_no_data_no_result}

wait :: proc (id: Job($R), state := _global) -> (res: R, found: bool) #optional_ok {
	found = _wait_raw(state, auto_cast id, &res)
	return
}

release :: proc (job: Job($R), state := _global) {
	_job_release_handle(state, auto_cast job)
}
invalidate :: proc (job: Job($R), state := _global) {
	_job_invalidate_handle(state, auto_cast job)
}

