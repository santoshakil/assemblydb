// AssemblyDB - Compression Port Adapter
// Wires LZ4 or no-op implementations into compress_port vtable

.include "src/const.s"

.text

// ============================================================================
// compress_adapter_init_lz4(vtable_ptr) -> void
// ============================================================================
.global compress_adapter_init_lz4
.type compress_adapter_init_lz4, %function
compress_adapter_init_lz4:
    adrp x1, lz4_compress
    add x1, x1, :lo12:lz4_compress
    str x1, [x0, #ZP_FN_COMPRESS]

    adrp x1, lz4_decompress
    add x1, x1, :lo12:lz4_decompress
    str x1, [x0, #ZP_FN_DECOMPRESS]

    adrp x1, lz4_max_compressed_size
    add x1, x1, :lo12:lz4_max_compressed_size
    str x1, [x0, #ZP_FN_MAX_SIZE]

    ret
.size compress_adapter_init_lz4, .-compress_adapter_init_lz4

// ============================================================================
// compress_adapter_init_noop(vtable_ptr) -> void
// ============================================================================
.global compress_adapter_init_noop
.type compress_adapter_init_noop, %function
compress_adapter_init_noop:
    adrp x1, noop_compress
    add x1, x1, :lo12:noop_compress
    str x1, [x0, #ZP_FN_COMPRESS]

    adrp x1, noop_decompress
    add x1, x1, :lo12:noop_decompress
    str x1, [x0, #ZP_FN_DECOMPRESS]

    adrp x1, noop_max_compressed_size
    add x1, x1, :lo12:noop_max_compressed_size
    str x1, [x0, #ZP_FN_MAX_SIZE]

    ret
.size compress_adapter_init_noop, .-compress_adapter_init_noop

.hidden lz4_compress
.hidden lz4_decompress
.hidden lz4_max_compressed_size
.hidden noop_compress
.hidden noop_decompress
.hidden noop_max_compressed_size
